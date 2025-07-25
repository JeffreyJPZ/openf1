import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Iterator, Literal, TypedDict

import pytz

from openf1.services.ingestor_livetiming.core.objects import (
    Collection,
    Document,
    Message,
)
from openf1.util.misc import deep_get, to_datetime, to_timedelta, add_timezone_info
    

class EventCategory(str, Enum):
    DRIVER_ACTION = "driver-action" # Actions by drivers - pits, outs, overtakes, hotlaps, track limits violations, incidents
    DRIVER_NOTIFICATION = "driver-notification" # Race control messsages to drivers - blue flags, black flags, black and white flags, black and orange flags, incident verdicts
    SECTOR_NOTIFICATION = "sector-notification" # Green (sector clear), yellow, double-yellow flags
    TRACK_NOTIFICATION = "track-notification" # Green (track clear) flags, red flags, chequered flags, safety cars, start of qualifying session parts


class EventCause(str, Enum):
    # Driver actions
    HOTLAP = "hotlap" # Used in qualifying/practice sessions - personal best laps resulting in position changes
    INCIDENT = "incident" # Collisions, unsafe rejoin, safety car/start infringements, etc.
    OFF_TRACK = "off-track" # Track limits violations
    OUT = "out"
    OVERTAKE = "overtake"
    PIT = "pit"
    
    # Driver notifications
    BLACK_FLAG = "black-flag"
    BLACK_AND_ORANGE_FLAG = "black-and-orange-flag"
    BLACK_AND_WHITE_FLAG = "black-and-white-flag"
    BLUE_FLAG = "blue-flag"
    INCIDENT_VERDICT = "incident-verdict" # Penalties, reprimands, no further investigations, etc.

    # Sector notifications
    GREEN_FLAG = "green-flag"
    YELLOW_FLAG = "yellow-flag"
    DOUBLE_YELLOW_FLAG = "double-yellow-flag"

    # Track notifications
    CHEQUERED_FLAG = "chequered-flag"
    RED_FLAG = "red-flag"
    SAFETY_CAR_DEPLOYED = "safety-car-deployed"
    VIRTUAL_SAFETY_CAR_DEPLOYED = "virtual-safety-car-deployed"
    SAFETY_CAR_ENDING = "safety-car-ending"
    VIRTUAL_SAFETY_CAR_ENDING = "virtual-safety-car-ending"
    Q1_START = "q1-start"
    Q2_START = "q2-start"
    Q3_START = "q3-start"


class EventDetails(TypedDict):
    """
    Event details can contain any of the following attributes:
    
    lap_number: int | None
        Describes lap numbers for events belonging to race sessions or the number of completed laps by a driver in practice/qualifying sessions.

    marker: str | dict[Literal['x', 'y', 'z'], int] | None
        Describes qualifying phases, turns and sectors for incident events, driver locations for overtake events.
        Examples:
            - 'Q1'
            - 'TURN 4'
            - 'SECTOR 13'
            - {'x': 1000, 'y': -150, 'z': 2}

    driver_roles: dict[str, Literal['initiator', 'participant']] | None
        Maps driver numbers to a role describing their involvement in the event:
            - 'initiator' if the driver was the main reason for the event (e.g. causing an incident)
            - 'participant' if the driver is merely involved in the event (e.g. being overtaken, being the victim of an incident).
        Events that only involve one driver (e.g. pit, out) will list the driver as the initiator

    position: int | None
        Describes the updated position on the timing board for a hotlap or overtake event.

    lap_duration: float | None
        The lap time, in seconds, for a hotlap.

    verdict: str | None
        The outcome of an incident.
        Examples:
            - 'REVIEWED NO FURTHER INVESTIGATION'
            - '10 SECOND TIME PENALTY'
            - 'DRIVE THROUGH PENALTY'

    reason: str | None
        The type of infringement for an incident.
        Examples:
            - 'REJOINING UNSAFELY'
            - 'CAUSING A COLLISION'
            - 'STARTING PROCEDURE INFRINGEMENT'

    message: str | None
        The full race control message for flag and incident events.

    compound: str | None
        The tyre compound for hotlap and pit events.

    tyre_age_at_start: int | None
        The number of laps for a tyre at the time of the event - used for hotlap and pit events.

    pit_duration: float | None
        The total time spent in the pit lane, in seconds, for a pit.
    
    """
    lap_number: int | None
    marker: str | dict[Literal["x", "y", "z"], int] | None
    driver_roles: dict[str, Literal["initiator", "participant"]] | None # Driver numbers must be kept as strings since they are part of a document
    position: int | None
    lap_duration: float | None
    verdict: str | None
    reason: str | None
    message: str | None
    compound: str | None
    tyre_age_at_start: int | None
    pit_duration: float | None


@dataclass(eq=False)
class Event(Document):
    meeting_key: int
    session_key: int
    date: datetime
    category: str
    cause: str
    details: EventDetails
    
    @property
    def unique_key(self) -> tuple:
        return (
            self.date,
            self.cause
        )


@dataclass
class EventsCollection(Collection):
    name = "events"
    source_topics = {
        "DriverRaceInfo",
        "LapCount",
        "PitLaneTimeCollection",
        "Position.z",
        "RaceControlMessages",
        "SessionInfo",
        "TimingAppData",
        "TimingData"
    }
    
    # Since messages are sorted by timepoint and then by topic we only need to keep the most recent data from other topics?
    session_start: datetime = field(default=None)
    session_offset: str = field(default=None) # GMT offset
    session_type: Literal["Practice", "Qualifying", "Race"] = field(default=None)
    lap_number: int = field(default=None)
    driver_positions: dict[int, dict[Literal["x", "y", "z"], int]] = field(default_factory=lambda: defaultdict(dict))
    # Combine latest stint data with latest pit data for pit event - stint number should be one more than pit number
    driver_stints: dict[int, dict[Literal["compound", "tyre_age_at_start"], int | str]] = field(default_factory=lambda: defaultdict(dict))
    driver_pits: dict[int, dict[Literal["date", "pit_duration", "lap_number"], datetime | float | int]] = field(default_factory=lambda: defaultdict(dict))


    def _update_lap_number(self, message: Message):
        # Update current lap number
        try:
            lap_number = int(deep_get(obj=message.content, key="CurrentLap"))
        except:
            return
        
        self.lap_number = lap_number


    def _update_session_info(self, message: Message):
        # Update session start and type
        # Not sure why deep_get doesn't work for session info message
        data = message.content
        
        gmt_offset = str(data.get("GmtOffset")) if data.get("GmtOffset") is not None else None
        session_type = str(data.get("Type")) if data.get("Type") is not None else None

        if gmt_offset is None or session_type is None:
            return
        
        try:
            session_start = to_datetime(str(data.get("StartDate")))
            session_start = add_timezone_info(dt=session_start, gmt_offset=gmt_offset)
        except:
            return
        
        self.session_start = session_start
        self.session_offset = gmt_offset
        self.session_type = session_type


    def _update_driver_position(self, driver_number: int, key: Literal["x", "y", "z"], value: int):
        driver_position = self.driver_positions[driver_number] 
        old_value = driver_position.get(key)
        if value != old_value:
            driver_position[key] = value
            

    def _update_driver_positions(self, message: Message):
        # Update driver positions using the latest values
        positions = deep_get(obj=message.content, key="Position")

        if not isinstance(positions, list):
            return
        
        latest_positions = positions[-1]

        if not isinstance(latest_positions, dict):
            return
        
        latest_entries = deep_get(obj=latest_positions, key="Entries")

        if not isinstance(latest_entries, dict):
            return
        
        for driver_number, data in latest_entries.items():
            try:
                driver_number = int(driver_number)
            except:
                continue
            
            if not isinstance(data, dict):
                continue
            
            try:
                x = int(data.get("X"))
                y = int(data.get("Y"))
                z = int(data.get("Z"))
            except:
                continue

            self._update_driver_position(
                driver_number=driver_number,
                key="x",
                value=x
            )
            self._update_driver_position(
                driver_number=driver_number,
                key="y",
                value=y
            )
            self._update_driver_position(
                driver_number=driver_number,
                key="z",
                value=z
            )

                
    def _update_driver_stint(self, driver_number: int, key: Literal["compound", "tyre_age_at_start"], value: bool | int | str):
        driver_stint = self.driver_stints[driver_number]
        old_value = driver_stint.get(key)
        if value != old_value:
            driver_stint[key] = value
            

    def _update_driver_stints(self, message: Message):
        # Update driver stints using the latest values
        stints = deep_get(obj=message.content, key="Lines")
        
        if not isinstance(stints, dict):
            return
        
        for driver_number, data in stints.items():
            try:
                driver_number = int(driver_number)
            except:
                continue
            
            if not isinstance(data, dict):
                continue

            driver_stints = deep_get(obj=data, key="Stints")

            if not isinstance(driver_stints, dict) or len(driver_stints.keys()) == 0:
                continue
            
            latest_stint_number = max(driver_stints.keys(), key=lambda stint_number: int(stint_number))
            latest_stint_data = driver_stints.get(latest_stint_number)

            if not isinstance(latest_stint_data, dict):
                continue
            
            # Conditional updates since not all stint messages are identical
            if "Compound" in latest_stint_data:
                compound = str(latest_stint_data.get("Compound"))

                self._update_driver_stint(
                    driver_number=driver_number,
                    key="compound",
                    value=compound
                )

            if "TotalLaps" in latest_stint_data:
                tyre_age_at_start = int(latest_stint_data.get("TotalLaps"))

                self._update_driver_stint(
                    driver_number=driver_number,
                    key="tyre_age_at_start",
                    value=tyre_age_at_start
                )
        

    def _update_driver_pit(self, driver_number: int, key: Literal["date", "pit_duration", "lap_number"], value: datetime | float | int):
        driver_pit = self.driver_pits[driver_number]
        old_value = driver_pit.get(key)
        if value != old_value:
            driver_pit[key] = value


    def _update_driver_pits(self, message: Message):
        # Update driver pits using the latest values
        pit_data = deep_get(obj=message.content, key="PitTimes")
        
        if not isinstance(pit_data, dict):
            return
        
        for driver_number, data in pit_data.items():
            try:
                driver_number = int(driver_number)
            except:
                continue
            
            if not isinstance(data, dict):
                continue

            try:
                pit_duration = float(data.get("Duration"))
                lap_number = int(data.get("Lap"))
            except:
                continue

            self._update_driver_pit(
                driver_number=driver_number,
                key="date",
                value=message.timepoint
            )
            self._update_driver_pit(
                driver_number=driver_number,
                key="pit_duration",
                value=pit_duration
            )
            self._update_driver_pit(
                driver_number=driver_number,
                key="lap_number",
                value=lap_number
            )


    def _process_hotlap(self, message: Message) -> Iterator[Event]:
        timing_data = deep_get(obj=message.content, key="Lines")

        if not isinstance(timing_data, dict):
            return
        
        print("Process potential hotlap")
        
        for driver_number, data in timing_data.items():
            try:
                driver_number = int(driver_number)
            except:
                continue

            if not isinstance(data, dict):
                continue

            # Check if "Position" and "BestLapTime" fields exist - this indicates a personal best hotlap
            if not "Position" in data or not "BestLapTime" in data:
                continue

            print(f"Personal best hotlap: {data}")

            try:
                position = int(data.get("Position"))
            except:
                position = None

            try:
                lap_time = to_timedelta(str(data.get("BestLapTime", {}).get("Value"))).total_seconds()
            except:
                lap_time = None

            details: EventDetails = {
                "driver_role": {f"{driver_number}": "initiator"},
                "position": position,
                "compound": self.driver_stints.get(driver_number, {}).get("compound"),
                "tyre_age_at_start": self.driver_stints.get(driver_number, {}).get("tyre_age_at_start"),
                "lap_duration": lap_time
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=message.timepoint,
                category=EventCategory.DRIVER_ACTION.value,
                cause=EventCause.HOTLAP.value,
                details=details
            )
    

    def _process_incident(self, message: Message) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            date = None

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = None
        
        # Extract incident information from race control message
        incident_pattern = (
            r"(?:FIA\s+STEWARDS:\s+)?"
            r"(?:(?P<marker>[A-Z0-9/\s]+?)\s+)?"                                                        # Captures marker if it exists
            r"(?:LAP\s+(?P<lap_number>\d+)\s+)?"                                                        # Captures lap number if it exists
            r"INCIDENT"
            r"(?:\s+INVOLVING\s+CARS?\s+(?P<driver_numbers>(?:\d+\s+\(\w+\)(?:\s*,\s*|\s+AND\s+)?)+))?" # Captures driver numbers if they exist
            r"\s+"
            r"NOTED"
            r"(?:\s+-\s+(?P<reason>.+))?"                                                               # Captures incident reason if it exists
        )
        match = re.search(pattern=incident_pattern, string=race_control_message)

        incident_marker = str(match.group("marker")) if match.group("marker") is not None else None
        incident_reason = str(match.group("reason")) if match.group("reason") is not None else None
        incident_lap_number = int(match.group("lap_number")) if match.group("lap_number") is not None else None

        try:
            incident_driver_numbers = [int(driver_number) for driver_number in re.findall(r"(\d+)", str(match.group("driver_numbers")))]
        except:
            incident_driver_numbers = []
        
        # Assume incidents between drivers specify a location and incidents between two or more drivers have driver at fault listed first,
        # since penalties can only be given if one driver is wholly or predominantly at fault?
        if len(incident_driver_numbers) == 0:
            # Incident does not specify drivers
            driver_roles = None
        elif len(incident_driver_numbers) >= 2 and incident_marker is not None:
            # Incident is between drivers, with the first listed driver at fault
            initiator_driver_number = incident_driver_numbers[0]
            participant_driver_numbers = incident_driver_numbers[1::]

            driver_roles = {
                **{f"{initiator_driver_number}": "initiator"},
                **{f"{driver_number}": "participant" for driver_number in participant_driver_numbers}
            }
        else:
            # Incident is not between drivers
            driver_roles = {f"{driver_number}": "initiator" for driver_number in incident_driver_numbers}

        details: EventDetails = {
            "lap_number": incident_lap_number if incident_lap_number is not None else lap_number,
            "marker": incident_marker,
            "reason": incident_reason,
            "message": race_control_message,
            "driver_roles": driver_roles
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            category=EventCategory.DRIVER_ACTION.value,
            cause=EventCause.INCIDENT.value,
            details=details
        )
    

    def _process_off_track(self, message: Message) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = None
        
        # Extract track violation information from race control message
        off_track_pattern = (
            r"CAR\s+(?P<driver_number>\d+).*?"      # Captures driver number
            r"AT\s+(?P<marker>[A-Z0-9/\s]+)\s+"     # Captures marker
            r"LAP\s+(?P<lap_number>\d+)\s+"         # Captures lap number
            r"(?P<time>\b\d{1,2}:\d{2}:\d{2}\b)"    # Captures local time
        )
        match = re.search(pattern=off_track_pattern, string=race_control_message)

        try:
            off_track_driver_number = int(match.group("driver_number"))
        except:
            off_track_driver_number = None
        
        try:
            off_track_marker = int(match.group("marker"))
        except:
            off_track_marker = None

        try:
            off_track_lap_number = int(match.group("lap_number"))
        except:
            off_track_lap_number = None

        try:
            off_track_time = str(match.group("time"))
            # Track limit violation time is local, need to convert to UTC
            date = datetime.combine(
                date=self.session_start.date(),
                time=datetime.strptime(off_track_time, "%H:%M:%S").time()
            )
            date = add_timezone_info(dt=date, gmt_offset=self.session_offset)
        except:
            date = None

        details: EventDetails = {
            "lap_number": off_track_lap_number if off_track_lap_number is None else lap_number, # Lap number for qualifying incidents is individual to driver
            "marker": off_track_marker,
            "message": race_control_message,
            "driver_roles": {f"{off_track_driver_number}": "initiator"} if off_track_driver_number is not None else None
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            category=EventCategory.DRIVER_ACTION.value,
            cause=EventCause.OFF_TRACK.value,
            details=details
        )
    

    def _process_out(self, message: Message) -> Iterator[Event]:
        for driver_number, data in message.content.items():
            try:
                driver_number = int(driver_number)
            except:
                continue

            if not isinstance(data, dict):
                continue

            if bool(data.get("IsOut")) is False:
                continue

            details: EventDetails = {
                "lap_number": self.lap_number,
                "marker": {
                    "x": self.driver_positions.get(driver_number, {}).get("x"),
                    "y": self.driver_positions.get(driver_number, {}).get("y"),
                    "z": self.driver_positions.get(driver_number, {}).get("z")
                },
                "driver_roles": {f"{driver_number}": "initiator"}
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=message.timepoint,
                category=EventCategory.DRIVER_ACTION.value,
                cause=EventCause.OUT.value,
                details=details
            )
        

    def _process_overtake(self, message: Message) -> Iterator[Event]:
        # Overtaking driver has OvertakeState equal to 2, overtaken drivers may or may not have OvertakeState
        try:
            overtaking_driver_number = next(
                (
                    int(driver_number)
                    for driver_number, data in message.content.items()
                    if isinstance(data, dict) and data.get("OvertakeState") == 2
                ),
                None,
            )
        except:
            overtaking_driver_number = None

        if overtaking_driver_number is None:
            # Not an overtake message
            return

        try:
            overtaken_driver_data = [
                (int(driver_number), int(data.get("Position")))
                for driver_number, data in message.content.items()
                if isinstance(data, dict)
                and data.get("OvertakeState") != 2
                and data.get("Position") is not None
            ]
        except:
            overtaken_driver_data = []

        if len(overtaken_driver_data) == 0:
            # Need at least two drivers to have an overtake
            return

        for overtaken_driver_number, position in overtaken_driver_data:
            # position is the overtaken driver's position after being overtaken, adjust position to account for this
            overtake_position = position - 1
        
            driver_roles = {
                **{f"{overtaking_driver_number}": "initiator"},
                **{f"{overtaken_driver_number}": "participant"}
            }
            
            details: EventDetails = {
                "lap_number": self.lap_number,
                "marker": {
                    "x": self.driver_positions.get(overtaking_driver_number, {}).get("x"),
                    "y": self.driver_positions.get(overtaking_driver_number, {}).get("y"),
                    "z": self.driver_positions.get(overtaking_driver_number, {}).get("z")
                },
                "position": overtake_position,
                "driver_roles": driver_roles
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=message.timepoint,
                category=EventCategory.DRIVER_ACTION.value,
                cause=EventCause.OVERTAKE.value,
                details=details
            )
    

    def _process_pit(self, message: Message) -> Iterator[Event]:
        # Use stint information to determine if a pit has occurred since pit information arrives before corresponding stint information
        # driver_pits should already be updated at this point
        stints = deep_get(obj=message.content, key="Lines")
        
        if not isinstance(stints, dict):
            return
        
        for driver_number, data in stints.items():
            try:
                driver_number = int(driver_number)
            except:
                continue
            
            if not isinstance(data, dict):
                continue

            driver_stints = deep_get(obj=data, key="Stints")

            if not isinstance(driver_stints, dict) or len(driver_stints.keys()) == 0:
                continue
            
            latest_stint_number = max(driver_stints.keys(), key=lambda stint_number: int(stint_number))
            latest_stint_data = driver_stints.get(latest_stint_number)

            if not isinstance(latest_stint_data, dict):
                continue
        
            if not "Compound" in latest_stint_data or latest_stint_data.get("TyresNotChanged") != "0":
                continue

            details: EventDetails = {
                "lap_number": self.driver_pits.get(driver_number, {}).get("lap_number"),
                "driver_roles": {f"{driver_number}": "initiator"},
                "compound": self.driver_stints.get(driver_number, {}).get("compound"),
                "tyre_age_at_start": self.driver_stints.get(driver_number, {}).get("tyre_age_at_start"),
                "pit_duration": self.driver_pits.get(driver_number, {}).get("pit_duration")
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=self.driver_pits.get(driver_number, {}).get("date"),
                category=EventCategory.DRIVER_ACTION.value,
                cause=EventCause.PIT.value,
                details=details
            )
    

    def _process_incident_verdict(self, message: Message) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = None
        
        # Extract incident verdict information from race control message
        # We need two patterns as penalty verdicts differ from others in structure
        incident_verdict_pattern = (
            r"(?:FIA\s+STEWARDS:\s+)?"
            r"(?:(?P<marker>[A-Z0-9/\s]+?)\s+)?"                                                        # Captures marker if it exists
            r"(?:LAP\s+(?P<lap_number>\d+)\s+)?"                                                        # Captures lap number if it exists
            r"INCIDENT"
            r"(?:\s+INVOLVING\s+CARS?\s+(?P<driver_numbers>(?:\d+\s+\(\w+\)(?:\s*,\s*|\s+AND\s+)?)+))?" # Captures driver numbers if they exist
            r"\s+"
            r"(?P<verdict>[^-]+?)"                                                                      # Captures verdict
            r"(?:\s*-\s*(?P<reason>.+))?"                                                               # Captures reason if it exists
        )

        penalty_verdict_pattern = (
            r"(?:FIA\s+STEWARDS:\s+)?"
            r"(?P<verdict>.+?)"                                                                         # Captures verdict
            r"\s+FOR\s+CAR\s+"
            r"(?P<driver_number>\d+)\s+\(\w+\)"                                                         # Captures driver number
            r"(?:\s*-\s*(?P<reason>.+))?"                                                               # Captures reason if it exists
        )

        match = re.search(pattern=incident_verdict_pattern, string=race_control_message)

        if match is not None:
            incident_verdict_marker = str(match.group("marker")) if match.group("marker") is not None else None
            incident_verdict = str(match.group("verdict")) if match.group("verdict") is not None else None
            incident_verdict_reason = str(match.group("reason")) if match.group("reason") is not None else None
            incident_verdict_lap_number = int(match.group("lap_number")) if match.group("lap_number") is not None else None
            
            try:
                incident_verdict_driver_numbers = [int(driver_number) for driver_number in re.findall(r"(\d+)", str(match.group("driver_numbers")))]
            except:
                incident_verdict_driver_numbers = []
            
            if len(incident_verdict_driver_numbers) == 0:
                # Incident does not specify drivers
                driver_roles = None
            elif len(incident_verdict_driver_numbers) >= 2 and incident_verdict_marker is not None:
                # Incident is between drivers, with the first listed driver at fault
                initiator_driver_number = incident_verdict_driver_numbers[0]
                participant_driver_numbers = incident_verdict_driver_numbers[1::]

                driver_roles = {
                    **{f"{initiator_driver_number}": "initiator"},
                    **{f"{driver_number}": "participant" for driver_number in participant_driver_numbers}
                }
            else:
                # Incident is not between drivers
                driver_roles = {f"{driver_number}": "initiator" for driver_number in incident_verdict_driver_numbers}

            details: EventDetails = {
                "lap_number": incident_verdict_lap_number if incident_verdict_lap_number is not None else lap_number,
                "marker": incident_verdict_marker,
                "verdict": incident_verdict,
                "reason": incident_verdict_reason,
                "message": race_control_message,
                "driver_roles": driver_roles
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=message.timepoint,
                category=EventCategory.DRIVER_NOTIFICATION.value,
                cause=EventCause.INCIDENT_VERDICT.value,
                details=details
            )
        else:
            match = re.search(pattern=penalty_verdict_pattern, string=race_control_message)

            incident_verdict = str(match.group("verdict")) if match.group("verdict") is not None else None
            incident_verdict_reason = str(match.group("reason")) if match.group("reason") is not None else None
            incident_verdict_driver_number = int(match.group("driver_number")) if match.group("driver_number") is not None else None
            
            details: EventDetails = {
                "verdict": incident_verdict,
                "reason": incident_verdict_reason,
                "message": race_control_message,
                "driver_roles": {f"{incident_verdict_driver_number}": "initiator"} if incident_verdict_driver_number is not None else None
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=message.timepoint,
                category=EventCategory.DRIVER_NOTIFICATION.value,
                cause=EventCause.INCIDENT_VERDICT.value,
                details=details
            )
    

    def _process_driver_flag(self, message: Message, event_cause: EventCause) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            date = None

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = None

        try:
            driver_number = int(deep_get(obj=message.content, key="RacingNumber"))
        except:
            driver_number = None
        
        # Black flags do not have "RacingNumber" field, need to extract driver number from race control message
        if driver_number is None and race_control_message is not None:
            driver_flag_pattern = r"CAR (?P<driver_number>\d+)"
            match = re.search(pattern=driver_flag_pattern, string=race_control_message)
            driver_number = int(match.group("driver_number")) if match.group("driver_number") is not None else None

        details: EventDetails = {
            "lap_number": lap_number,
            "message": race_control_message,
            "driver_roles": {f"{driver_number}": "initiator"} if driver_number is not None else None
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            category=EventCategory.DRIVER_NOTIFICATION.value,
            cause=event_cause.value,
            details=details
        )
    

    def _process_sector_flag(self, message: Message, event_cause: EventCause) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return

        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            date = None

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = None
        
        # Extract sector from race control message

        sector_pattern = r"(?P<marker>SECTOR\s+\d+)"
        match = re.search(pattern=sector_pattern, string=race_control_message)
        sector_marker = str(match.group("marker")) if match.group("marker") is not None else None
        
        details: EventDetails = {
            "lap_number": lap_number,
            "marker": sector_marker,
            "message": race_control_message
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            category=EventCategory.SECTOR_NOTIFICATION.value,
            cause=event_cause.value,
            details=details
        )
    

    def _process_track_flag(self, message: Message, event_cause: EventCause) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return

        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            date = None

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = None
        
        details: EventDetails = {
            "lap_number": lap_number,
            "message": race_control_message
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            category=EventCategory.TRACK_NOTIFICATION.value,
            cause=event_cause.value,
            details=details
        )
    

    def _process_qualifying_part_start(self, message: Message, event_cause: EventCause) -> Iterator[Event]:
        print(f"Qualifying part start: {event_cause.value}")

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=message.timepoint,
            category=EventCategory.TRACK_NOTIFICATION.value,
            cause=event_cause.value,
            details=None
        )


    # Maps event causes to unique conditions that determine if event messages belong to that cause
    # message should be of type Message
    def _get_event_condition_map(self) -> dict[EventCause, Callable[..., bool]]:
        return {
            EventCause.HOTLAP: lambda message: all(cond() for cond in [
                lambda: message.topic == "TimingData", 
                lambda: self.session_type in ["Practice", "Qualifying"],
                lambda: deep_get(obj=message.content, key="SessionPart") is None, # Avoid qualifying part messages that also have the fields below
                lambda: deep_get(obj=message.content, key="BestLapTime"),
                lambda: deep_get(obj=message.content, key="Position")
            ]),
            EventCause.INCIDENT: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str),
                lambda: "INCIDENT" in deep_get(obj=message.content, key="Message"),
                lambda: "NOTED" in deep_get(obj=message.content, key="Message")
            ]),
            EventCause.OFF_TRACK: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str),
                lambda: "TRACK LIMITS" in deep_get(obj=message.content, key="Message")
            ]),
            EventCause.OUT: lambda message: all(cond() for cond in [
                lambda: message.topic == "DriverRaceInfo",
                lambda: self.session_type == "Race",
                lambda: bool(deep_get(obj=message.content, key="IsOut")) is True
            ]),
            EventCause.OVERTAKE: lambda message: all(cond() for cond in [
                lambda: message.topic == "DriverRaceInfo",
                lambda: self.session_type == "Race",
                lambda: deep_get(obj=message.content, key="OvertakeState") is not None,
                lambda: deep_get(obj=message.content, key="Position") is not None
            ]),
            EventCause.PIT: lambda message: all(cond() for cond in [
                lambda: message.topic == "TimingAppData",
                lambda: self.session_type == "Race",
                lambda: isinstance(deep_get(obj=message.content, key="Compound"), str),
                lambda: deep_get(obj=message.content, key="Compound") != "UNKNOWN",
                lambda: deep_get(obj=message.content, key="TyresNotChanged") == "0" # TyresNotChanged being 0 indicates that pit stop is complete and tyre compound is known
            ]),

            EventCause.BLACK_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str), # Check that message is a str to avoid TypeError when searching for substring
                lambda: "BLACK" in deep_get(obj=message.content, key="Message")
            ]), # Black flags do not have a "Flag" field
            EventCause.BLACK_AND_ORANGE_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str),
                lambda: "BLACK AND ORANGE" in deep_get(obj=message.content, key="Message")
            ]),
            EventCause.BLACK_AND_WHITE_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str),
                lambda: "BLACK AND WHITE" in deep_get(obj=message.content, key="Message")
            ]),
            EventCause.BLUE_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: deep_get(obj=message.content, key="Flag") == "BLUE"
            ]),
            EventCause.INCIDENT_VERDICT: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str),
                lambda: "FIA STEWARDS" in deep_get(obj=message.content, key="Message"),
                lambda: "UNDER INVESTIGATION" not in deep_get(obj=message.content, key="Message"), # "UNDER INVESTIGATION" is not a verdict
                lambda: "WILL BE INVESTIGATED AFTER THE RACE" not in deep_get(obj=message.content, key="Message"), # "WILL BE INVESTIGATED AFTER THE RACE" is not a verdict
                lambda: "WILL BE INVESTIGATED AFTER THE SESSION" not in deep_get(obj=message.content, key="Message") # "WILL BE INVESTIGATED AFTER THE SESSION" is not a verdict
            ]),

            EventCause.GREEN_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages"
            ]) and any(cond() for cond in [
                lambda: deep_get(obj=message.content, key="Flag") == "GREEN",
                lambda: deep_get(obj=message.content, key="Flag") == "CLEAR",
            ]),
            EventCause.YELLOW_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: deep_get(obj=message.content, key="Flag") == "YELLOW"
            ]),
            EventCause.DOUBLE_YELLOW_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: deep_get(obj=message.content, key="Flag") == "DOUBLE YELLOW"
            ]),
            
            EventCause.CHEQUERED_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: deep_get(obj=message.content, key="Flag") == "CHEQUERED"
            ]),
            EventCause.RED_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: deep_get(obj=message.content, key="Flag") == "RED"
            ]),
            EventCause.SAFETY_CAR_DEPLOYED: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: self.session_type == "Race",
                lambda: deep_get(obj=message.content, key="Category") == "SafetyCar",
                lambda: deep_get(obj=message.content, key="Mode") == "SAFETY CAR",
                lambda: deep_get(obj=message.content, key="Status") == "DEPLOYED"
            ]),
            EventCause.VIRTUAL_SAFETY_CAR_DEPLOYED: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: self.session_type == "Race",
                lambda: deep_get(obj=message.content, key="Category") == "SafetyCar",
                lambda: deep_get(obj=message.content, key="Mode") == "VIRTUAL SAFETY CAR",
                lambda: deep_get(obj=message.content, key="Status") == "DEPLOYED"
            ]),
            EventCause.SAFETY_CAR_ENDING: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: self.session_type == "Race",
                lambda: deep_get(obj=message.content, key="Category") == "SafetyCar",
                lambda: deep_get(obj=message.content, key="Mode") == "SAFETY CAR",
                lambda: deep_get(obj=message.content, key="Status") == "IN THIS LAP"
            ]),
            EventCause.VIRTUAL_SAFETY_CAR_ENDING: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: self.session_type == "Race",
                lambda: deep_get(obj=message.content, key="Category") == "SafetyCar",
                lambda: deep_get(obj=message.content, key="Mode") == "VIRTUAL SAFETY CAR",
                lambda: deep_get(obj=message.content, key="Status") == "ENDING"
            ]),
            EventCause.Q1_START: lambda message: all(cond() for cond in [
                lambda: message.topic == "TimingData",
                lambda: self.session_type == "Qualifying",
                lambda: deep_get(obj=message.content, key="SessionPart") == 1
            ]),
            EventCause.Q2_START: lambda message: all(cond() for cond in [
                lambda: message.topic == "TimingData",
                lambda: self.session_type == "Qualifying",
                lambda: deep_get(obj=message.content, key="SessionPart") == 2
            ]),
            EventCause.Q3_START: lambda message: all(cond() for cond in [
                lambda: message.topic == "TimingData",
                lambda: self.session_type == "Qualifying",
                lambda: deep_get(obj=message.content, key="SessionPart") == 3
            ])
        }

    
    # Maps event causes to specific processing logic
    # message should be of type Message
    def _get_event_processing_map(self) -> dict[EventCause, Callable[..., Iterator[Event]]]:
        return {
            EventCause.HOTLAP: lambda message: self._process_hotlap(message),
            EventCause.INCIDENT: lambda message: self._process_incident(message),
            EventCause.OFF_TRACK: lambda message: self._process_off_track(message),
            EventCause.OUT: lambda message: self._process_out(message),
            EventCause.OVERTAKE: lambda message: self._process_overtake(message),
            EventCause.PIT: lambda message: self._process_pit(message),
            
            EventCause.BLACK_FLAG: lambda message: self._process_driver_flag(message=message, event_cause=EventCause.BLACK_FLAG),
            EventCause.BLACK_AND_ORANGE_FLAG: lambda message: self._process_driver_flag(message=message, event_cause=EventCause.BLACK_AND_ORANGE_FLAG),
            EventCause.BLACK_AND_WHITE_FLAG: lambda message: self._process_driver_flag(message=message, event_cause=EventCause.BLACK_AND_WHITE_FLAG),
            EventCause.BLUE_FLAG: lambda message: self._process_driver_flag(message=message, event_cause=EventCause.BLUE_FLAG),
            EventCause.INCIDENT_VERDICT: lambda message: self._process_incident_verdict(message),

            EventCause.GREEN_FLAG: (
                lambda message: self._process_sector_flag(message=message, event_cause=EventCause.GREEN_FLAG) 
                if deep_get(obj=message.content, key="Scope") is not None and deep_get(obj=message.content, key="Scope") == "Sector"
                else self._process_track_flag(message=message, event_cause=EventCause.GREEN_FLAG)
            ),
            EventCause.YELLOW_FLAG: lambda message: self._process_sector_flag(message=message, event_cause=EventCause.YELLOW_FLAG),
            EventCause.DOUBLE_YELLOW_FLAG: lambda message: self._process_sector_flag(message=message, event_cause=EventCause.DOUBLE_YELLOW_FLAG),

            EventCause.CHEQUERED_FLAG: lambda message: self._process_track_flag(message=message, event_cause=EventCause.CHEQUERED_FLAG),
            EventCause.RED_FLAG: lambda message: self._process_track_flag(message=message, event_cause=EventCause.RED_FLAG),
            EventCause.SAFETY_CAR_DEPLOYED: lambda message: self._process_track_flag(message=message, event_cause=EventCause.SAFETY_CAR_DEPLOYED),
            EventCause.VIRTUAL_SAFETY_CAR_DEPLOYED: lambda message: self._process_track_flag(message=message, event_cause=EventCause.VIRTUAL_SAFETY_CAR_DEPLOYED),
            EventCause.SAFETY_CAR_ENDING: lambda message: self._process_track_flag(message=message, event_cause=EventCause.SAFETY_CAR_ENDING),
            EventCause.VIRTUAL_SAFETY_CAR_ENDING: lambda message: self._process_track_flag(message=message, event_cause=EventCause.VIRTUAL_SAFETY_CAR_ENDING),
            EventCause.Q1_START: lambda message: self._process_qualifying_part_start(message=message, event_cause=EventCause.Q1_START),
            EventCause.Q2_START: lambda message: self._process_qualifying_part_start(message=message, event_cause=EventCause.Q2_START),
            EventCause.Q3_START: lambda message: self._process_qualifying_part_start(message=message, event_cause=EventCause.Q3_START)
        }


    def process_message(self, message: Message) -> Iterator[Event]:
        match (message.topic):
            case "LapCount":
                self._update_lap_number(message)
            case "PitLaneTimeCollection":
                self._update_driver_pits(message)
            case "Position.z":
                self._update_driver_positions(message)
            case "SessionInfo":
                self._update_session_info(message)
            case "TimingAppData":
                self._update_driver_stints(message)
            case _:
                pass
            
        # Find event cause corresponding to message
        event_cause = next(
            (event_cause for event_cause, cond in self._get_event_condition_map().items()
                if cond(message)
            ),
            None
        )

        if event_cause is None:
            # Not an event message
            return
        
        yield from self._get_event_processing_map().get(event_cause)(message)

        