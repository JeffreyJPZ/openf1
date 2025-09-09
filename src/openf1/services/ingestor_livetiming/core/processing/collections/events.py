import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, ClassVar, Iterator, Literal, TypedDict

import pytz

from openf1.services.ingestor_livetiming.core.objects import (
    Collection,
    Document,
    Message,
)
from openf1.util.misc import deep_get, to_datetime, to_timedelta, add_timezone_info


def _hash_obj(obj: dict) -> str:
    """
    Returns a SHA3-512 hashed value representing a dictionary.
    """
    return hashlib.sha3_512(json.dumps(obj=obj, sort_keys=True).encode("utf-8")).hexdigest()


def _get_elapsed_time(start: datetime, end: datetime) -> str | None:
    """
    Returns the elapsed time between start and end as a HH:MM:SS formatted string,
    otherwise None.
    Assumes start and end are on the same day.
    If end < start, returns the formatting string with a '-' symbol prefixed.
    """
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return None
    
    # Handles underflow.
    if end < start:
        # Pad an extra 0 in front for single digit hours.
        return f"-{re.sub(r'^(\d):', r'0\1:', str(start - end))}"
    
    return re.sub(r'^(\d):', r'0\1:', str(end - start))


class EventCategory(str, Enum):
    DRIVER_ACTION = "driver-action" # Actions by drivers - pit, out, overtakes, personal best laps, track limits violations, incidents
    DRIVER_NOTIFICATION = "driver-notification" # Other events involving drivers - blue flags, black flags, black and white flags, black and orange flags, incident verdicts, qualifying stage classifications, provisional classifications
    SECTOR_NOTIFICATION = "sector-notification" # Green (sector clear), yellow, double-yellow flags
    TRACK_NOTIFICATION = "track-notification" # Green (track clear) flags, red flags, chequered flags, safety cars
    SESSION_NOTIFICATION = "session-notification" # Session start/end/pause/resume, practice start/end, qualifying stage start/end, race start/end
    OTHER = "other" # Other types of events not covered by the other categories - race control messages

class EventCause(str, Enum):
    # Driver actions
    PERSONAL_BEST_LAP = "personal-best-lap" # Used in qualifying/practice sessions - personal best laps
    INCIDENT = "incident" # Collisions, unsafe rejoin, safety car/start infringements, etc.
    TRACK_LIMITS = "track-limits" # Track limits violations
    OUT = "out"
    OVERTAKE = "overtake"
    PIT = "pit"
    
    # Driver notifications
    BLACK_FLAG = "black-flag"
    BLACK_AND_ORANGE_FLAG = "black-and-orange-flag"
    BLACK_AND_WHITE_FLAG = "black-and-white-flag"
    BLUE_FLAG = "blue-flag"
    INCIDENT_VERDICT = "incident-verdict" # Penalties, reprimands, no further investigations, etc.
    PROVISIONAL_CLASSIFICATION = "provisional-classification" # Driver positions at the end of the session
    QUALIFYING_STAGE_CLASSIFICATION = "qualifying-stage-classification" # Used in qualifying sessions - whether a driver was eliminated or advanced in a qualifying stage (Q1, Q2, SQ1, SQ2)

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

    # Session notifications
    SESSION_START = "session-start"
    SESSION_END = "session-end"
    SESSION_STOP = "session-stop"
    SESSION_RESUME = "session-resume"
    PRACTICE_START = "practice-start"
    PRACTICE_END = "practice-end"
    Q1_START = "q1-start"
    Q1_END = "q1-end"
    Q2_START = "q2-start"
    Q2_END = "q2-end"
    Q3_START = "q3-start"
    Q3_END = "q3-end"
    RACE_START = "race-start"
    RACE_END = "race-end"

    # Other
    RACE_CONTROL_MESSAGE = "race-control-message"


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
            - 'initiator' if the driver is the main subject or is directly responsible for the event (e.g. causing an incident)
            - 'participant' if the driver is merely involved in the event (e.g. being overtaken, being the victim of an incident).
        Events that only involve one driver (e.g. pit, out) will list the driver as the initiator

    position: int | None
        Describes the latest position on the timing board - used for personal best lap, overtake, provisional and qualifying stage classification events.

    lap_duration: float | None
        The lap time, in seconds - used for personal best lap, provisional and qualifying stage classification events.

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
        The tyre compound for personal best lap, pit, provisional and qualifying stage classification events.

    tyre_age_at_start: int | None
        The number of laps for a tyre at the time of the event - used for personal best lap, pit, provisional and qualifying stage classification events.

    pit_lane_duration: float | None
        The total time spent in the pit lane, in seconds, for a pit.

    pit_stop_duration: float | None
        The total time spent stationary in the pit box, in seconds, for a pit.

    qualifying_stage_number: Literal[1, 2] | None
        The number of the qualifying stage (1, 2) where drivers can be eliminated - used for qualifying stage classification events.

    eliminated: bool | None
        Whether a driver was eliminated or advanced in a qualifying stage - used for qualifying stage classification events.
    
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
    pit_lane_duration: float | None
    pit_stop_duration: float | None
    qualifying_stage_number: Literal[1, 2] | None
    eliminated: bool | None


@dataclass(eq=False)
class Event(Document):
    meeting_key: int
    session_key: int
    date: datetime
    elapsed_time: str # HH:MM:SS formatted string for the time between the session stream start and the given event
    category: str
    cause: str
    details: EventDetails
    
    @property
    def unique_key(self) -> tuple:
        return (
            self.date,
            self.cause,
            _hash_obj(self.details)
        )


@dataclass
class EventsCollection(Collection):
    name = "events"
    source_topics = {
        "DriverRaceInfo",
        "LapCount",
        "PitLaneTimeCollection",
        "PitStopSeries", # New topic with stationary time for pit stops
        "Position.z",
        "RaceControlMessages",
        "SessionData",
        "SessionInfo",
        "TimingAppData",
        "TimingData"
    }

    # Used to select between the old "PitLaneTimeCollection" and new "PitStopSeries" pit data topic,
    # where the new topic contains pit stationary time and first appears during the 2024 US GP weekend,
    # only for races
    NEW_RACE_PIT_TOPIC_CUTOFF_DATE: ClassVar[datetime] = add_timezone_info(
        dt=datetime(year=2024, month=10, day=18, hour=12, minute=30, second=0),
        gmt_offset="-05:00:00"
    )

    session_stream_start: datetime = field(default=None)

    # Since messages are sorted by timepoint and then by topic we only need to keep the most recent data from other topics?
    session_start: datetime = field(default=None)
    session_offset: str = field(default=None) # GMT offset
    session_type: Literal["Practice", "Qualifying", "Race"] = field(default=None)
    session_status: Literal["Aborted", "Ends", "Finalised", "Finished", "Started"] = field(default=None) # Track session state to help create session notifications

    lap_number: int = field(default=None)
    qualifying_stage_number: Literal[1, 2, 3] = field(default=None) # Track qualifying stage number to help create session notifications

    # Combine latest stint data with latest pit data for pit event - stint number should be one more than pit number
    driver_stints: dict[int, dict[Literal["compound", "tyre_age_at_start"], int | str]] = field(default_factory=lambda: defaultdict(dict))
    driver_pits: dict[int, dict[Literal["date", "pit_lane_duration", "pit_stop_duration", "lap_number"], datetime | float | int]] = field(default_factory=lambda: defaultdict(dict))

    # Track latest driver locations
    driver_locations: dict[int, dict[Literal["x", "y", "z"], int]] = field(default_factory=lambda: defaultdict(dict))

    # Track driver personal best times for qualifying
    driver_personal_best_laps: dict[int, float] = field(default_factory=lambda: defaultdict(float))

    # Track latest driver positions
    driver_positions: dict[int, int] = field(default_factory=lambda: defaultdict(int))


    def _update_lap_number(self, message: Message):
        # Update current lap number
        try:
            lap_number = int(deep_get(obj=message.content, key="CurrentLap"))
        except:
            return
        
        self.lap_number = lap_number


    def _update_session_stream_start(self, message: Message):
        # Update session stream start if message indicates the session stream has started
        cond = self._get_event_condition_map().get(EventCause.SESSION_START)
        if cond is not None and cond(message):
            self.session_stream_start = message.timepoint
            

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

    
    def _update_session_status(self, message: Message):
        try:
            session_status = str(deep_get(obj=message.content, key="SessionStatus"))
        except:
            return
        
        if session_status not in ["Aborted", "Ends", "Finalised", "Finished", "Started"]:
            # Ignore "Inactive" status since it conflicts with event condition logic
            return
        
        self.session_status = session_status

    
    def _update_qualifying_stage_number(self, message: Message):
        try:
            qualifying_stage_number = int(deep_get(obj=message.content, key="QualifyingPart"))
        except:
            return
        
        if qualifying_stage_number not in [1, 2, 3]:
            return
        
        self.qualifying_stage_number = qualifying_stage_number


    def _update_driver_location(
            self, driver_number: int,
            key: Literal["x", "y", "z"],
            value: int
        ):
        driver_position = self.driver_locations[driver_number] 
        old_value = driver_position.get(key)
        if value != old_value:
            driver_position[key] = value
            

    def _update_driver_locations(self, message: Message):
        # Update driver locations using the latest values
        locations = deep_get(obj=message.content, key="Position")

        if not isinstance(locations, list):
            return
        
        latest_locations = locations[-1]

        if not isinstance(latest_locations, dict):
            return
        
        latest_entries = deep_get(obj=latest_locations, key="Entries")

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

            self._update_driver_location(
                driver_number=driver_number,
                key="x",
                value=x
            )
            self._update_driver_location(
                driver_number=driver_number,
                key="y",
                value=y
            )
            self._update_driver_location(
                driver_number=driver_number,
                key="z",
                value=z
            )

                
    def _update_driver_stint(
            self,
            driver_number: int,
            key: Literal["compound", "tyre_age_at_start"],
            value: bool | int | str
        ):
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

    
    def _update_driver_personal_best_lap(
            self,
            driver_number: int,
            value: float
        ):
        old_value = self.driver_personal_best_laps.get(driver_number)
        if value != old_value:
            self.driver_personal_best_laps[driver_number] = value


    def _update_driver_personal_best_laps(self, message: Message) -> Iterator[Event]:
        # Update driver personal best lap times using the latest values
        timing_data = deep_get(obj=message.content, key="Lines")

        if not isinstance(timing_data, dict):
            return
        
        for driver_number, data in timing_data.items():
            try:
                driver_number = int(driver_number)
            except:
                continue

            if not isinstance(data, dict):
                continue

            try:
                best_lap_time = to_timedelta(str(data.get("BestLapTime", {}).get("Value"))).total_seconds()
            except:
                continue

            try:
                last_lap_time = to_timedelta(str(data.get("LastLapTime", {}).get("Value"))).total_seconds()
            except:
                continue

            # Check for and compare lap times (up to thousandths precision)
            if math.isclose(a=best_lap_time, b=last_lap_time, rel_tol=1e-3):
                self._update_driver_personal_best_lap(driver_number=driver_number, value=best_lap_time)
        

    def _update_driver_pit(
            self,
            driver_number: int,
            key: Literal["date", "pit_lane_duration", "pit_stop_duration", "lap_number"],
            value: datetime | float | int
        ):
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
            
            pit_lane_duration = deep_get(obj=data, key="PitLaneTime") or deep_get(obj=data, key="Duration")
            lap_number = deep_get(obj=data, key="Lap")

            if pit_lane_duration is None or lap_number is None:
                # Not a pit message
                continue
            
            try:
                pit_lane_duration = float(pit_lane_duration)
                lap_number = int(lap_number)
            except:
                continue

            self._update_driver_pit(
                driver_number=driver_number,
                key="date",
                value=message.timepoint
            )
            self._update_driver_pit(
                driver_number=driver_number,
                key="pit_lane_duration",
                value=pit_lane_duration
            )
            self._update_driver_pit(
                driver_number=driver_number,
                key="lap_number",
                value=lap_number
            )

            # From 2025 onwards, get the stationary time if it exists
            pit_stop_duration = deep_get(obj=data, key="PitStopTime")

            if pit_stop_duration is not None:
                try:
                    pit_stop_duration = float(pit_stop_duration)
                except:
                    continue

                self._update_driver_pit(
                    driver_number=driver_number,
                    key="pit_stop_duration",
                    value=pit_stop_duration
                )


    def _update_driver_position(
            self,
            driver_number: int,
            value: int
        ):
        old_value = self.driver_positions.get(driver_number)
        if value != old_value:
            self.driver_positions[driver_number] = value


    def _update_driver_positions(self, message: Message):
        # Update driver positions using the latest values
        position_data = deep_get(obj=message.content, key="Lines")
        
        if not isinstance(position_data, dict):
            return
        
        for driver_number, data in position_data.items():
            try:
                driver_number = int(driver_number)
            except:
                continue
            
            if not isinstance(data, dict):
                continue

            try:
                position = int(data.get("Position"))
            except:
                continue
            
            self._update_driver_position(driver_number=driver_number, value=position)

    
    def _process_incident(self, message: Message) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            # Use UTC date as fallback
            date = message.timepoint

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            # Use internal lap number as fallback
            lap_number = self.lap_number
        
        # Extract incident information from race control message
        incident_pattern = (
            r"^"
            r"(?:FIA\s+STEWARDS:\s+)?"
            r"(?:(?P<marker>[A-Z0-9/\s]+?)\s+)?"                                                        # Captures marker if it exists
            r"(?:LAP\s+(?P<lap_number>\d+)\s+)?"                                                        # Captures lap number if it exists
            r"INCIDENT"
            r"(?:\s+INVOLVING\s+CARS?\s+(?P<driver_numbers>(?:\d+\s+\(\w+\)(?:\s*,\s*|\s+AND\s+)?)+))?" # Captures driver numbers if they exist
            r"\s+"
            r"NOTED"
            r"(?:\s+-\s+(?P<reason>.+))?"                                                               # Captures incident reason if it exists
            r"$"
        )
        match = re.search(pattern=incident_pattern, string=race_control_message)

        if match is None:
            return
        
        incident_marker = str(match.group("marker")) if match.group("marker") is not None else None
        incident_reason = str(match.group("reason")) if match.group("reason") is not None else None
        incident_lap_number = int(match.group("lap_number")) if match.group("lap_number") is not None else None

        try:
            incident_driver_numbers = [
                int(driver_number) for driver_number in re.findall(r"(\d+)", str(match.group("driver_numbers")))
            ]
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

        # Prioritize lap number in message
        lap_number = incident_lap_number if incident_lap_number is not None else lap_number

        details: EventDetails = {
            "lap_number": lap_number,
            "marker": incident_marker,
            "driver_roles": driver_roles,
            "reason": incident_reason,
            "message": race_control_message
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
            category=EventCategory.DRIVER_ACTION.value,
            cause=EventCause.INCIDENT.value,
            details=details
        )
    

    def _process_outs(self, message: Message) -> Iterator[Event]:
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
                    "x": self.driver_locations.get(driver_number, {}).get("x"),
                    "y": self.driver_locations.get(driver_number, {}).get("y"),
                    "z": self.driver_locations.get(driver_number, {}).get("z")
                },
                "driver_roles": {f"{driver_number}": "initiator"}
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=message.timepoint,
                elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=message.timepoint),
                category=EventCategory.DRIVER_ACTION.value,
                cause=EventCause.OUT.value,
                details=details
            )
        

    def _process_overtakes(self, message: Message) -> Iterator[Event]:
        # Overtaking driver has "OvertakeState" equal to 2, overtaken drivers may or may not have "OvertakeState"
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
                    "x": self.driver_locations.get(overtaking_driver_number, {}).get("x"),
                    "y": self.driver_locations.get(overtaking_driver_number, {}).get("y"),
                    "z": self.driver_locations.get(overtaking_driver_number, {}).get("z")
                },
                "driver_roles": driver_roles,
                "position": overtake_position
            }

            # Adjust date so that multiple overtakes do not have the exact same date
            # (overtakes for lower positions are before overtakes for higher positions when sorting chronologically by date)
            # Since F1 only has millisecond precision timing this is inconsequential
            date = message.timepoint - timedelta(microseconds=overtake_position)
            
            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=date, 
                elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
                category=EventCategory.DRIVER_ACTION.value,
                cause=EventCause.OVERTAKE.value,
                details=details
            )

    
    def _process_personal_best_laps(self, message: Message) -> Iterator[Event]:
        timing_data = deep_get(obj=message.content, key="Lines")

        if not isinstance(timing_data, dict):
            return
        
        for driver_number, data in timing_data.items():
            try:
                driver_number = int(driver_number)
            except:
                continue

            if not isinstance(data, dict):
                continue
            
            try:
                best_lap_time = to_timedelta(str(data.get("BestLapTime", {}).get("Value"))).total_seconds()
            except:
                continue

            try:
                last_lap_time = to_timedelta(str(data.get("LastLapTime", {}).get("Value"))).total_seconds()
            except:
                continue

            try:
                position = int(data.get("Position"))
            except:
                position = None

            # Check for and compare lap times (up to thousandths precision)
            if position is not None and math.isclose(a=best_lap_time, b=last_lap_time, rel_tol=1e-3):
                # If "Position" field exists and "BestLapTime" and "LastLapTime" values are equal,
                # then driver has set a personal best lap resulting in a position change
                details: EventDetails = {
                    "driver_roles": {f"{driver_number}": "initiator"},
                    "position": position,
                    "lap_duration": best_lap_time,
                    "compound": self.driver_stints.get(driver_number, {}).get("compound"),
                    "tyre_age_at_start": self.driver_stints.get(driver_number, {}).get("tyre_age_at_start")
                }

                yield Event(
                    meeting_key=self.meeting_key,
                    session_key=self.session_key,
                    date=message.timepoint,
                    elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=message.timepoint),
                    category=EventCategory.DRIVER_ACTION.value,
                    cause=EventCause.PERSONAL_BEST_LAP.value,
                    details=details
                )
            elif math.isclose(a=best_lap_time, b=last_lap_time, rel_tol=1e-3):
                # If only "BestLapTime" and "LastLapTime" values are equal, then driver has set a personal best lap,
                # but no change in position
                details: EventDetails = {
                    "driver_roles": {f"{driver_number}": "initiator"},
                    "lap_duration": best_lap_time,
                    "compound": self.driver_stints.get(driver_number, {}).get("compound"),
                    "tyre_age_at_start": self.driver_stints.get(driver_number, {}).get("tyre_age_at_start")
                }

                yield Event(
                    meeting_key=self.meeting_key,
                    session_key=self.session_key,
                    date=message.timepoint,
                    elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=message.timepoint),
                    category=EventCategory.DRIVER_ACTION.value,
                    cause=EventCause.PERSONAL_BEST_LAP.value,
                    details=details
                )
    

    def _process_pits(self, message: Message) -> Iterator[Event]:
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
        
            if not "Compound" in latest_stint_data or latest_stint_data.get("Compound") == "UNKNOWN" or latest_stint_data.get("TyresNotChanged") != "0":
                continue
            
            # Prioritize date from pit information
            date = self.driver_pits.get(driver_number, {}).get("date") if self.driver_pits.get(driver_number, {}).get("date") is not None else message.timepoint
            lap_number = self.driver_pits.get(driver_number, {}).get("lap_number") if self.driver_pits.get(driver_number, {}).get("lap_number") is not None else self.lap_number

            details: EventDetails = {
                "lap_number": lap_number,
                "driver_roles": {f"{driver_number}": "initiator"},
                "compound": self.driver_stints.get(driver_number, {}).get("compound"),
                "tyre_age_at_start": self.driver_stints.get(driver_number, {}).get("tyre_age_at_start"),
                "pit_lane_duration": self.driver_pits.get(driver_number, {}).get("pit_lane_duration"),
                "pit_stop_duration": self.driver_pits.get(driver_number, {}).get("pit_stop_duration")
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=date,
                elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
                category=EventCategory.DRIVER_ACTION.value,
                cause=EventCause.PIT.value,
                details=details
            )

    
    def _process_track_limits(self, message: Message) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            date = message.timepoint

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = self.lap_number
        
        # Extract track limits violation information from race control message
        track_limits_pattern = (
            r"^"
            r"CAR\s+(?P<driver_number>\d+).*?"      # Captures driver number
            r"AT\s+(?P<marker>[A-Z0-9/\s]+)\s+"     # Captures marker
            r"LAP\s+(?P<lap_number>\d+)\s+"         # Captures lap number
            r"(?P<time>\b\d{1,2}:\d{2}:\d{2}\b).*"  # Captures local time
            r"$"
        )
        match = re.search(pattern=track_limits_pattern, string=race_control_message)
        
        if match is None:
            return
        
        track_limits_driver_number = int(match.group("driver_number")) if match.group("driver_number") is not None else None
        track_limits_marker = str(match.group("marker")) if match.group("marker") is not None else None
        track_limits_lap_number = int(match.group("lap_number")) if match.group("lap_number") is not None else None
        track_limits_time = str(match.group("time")) if match.group("time") is not None else None

        try:
            # Track limits violation time is local, need to convert to UTC
            track_limits_date = datetime.combine(
                date=date.date(),
                time=datetime.strptime(track_limits_time, "%H:%M:%S").time()
            )
            track_limits_date = add_timezone_info(dt=track_limits_date, gmt_offset=self.session_offset)
        except:
            track_limits_date = None

        # Prioritize information in message
        date = track_limits_date if track_limits_date is not None else date
        lap_number = track_limits_lap_number if track_limits_lap_number is not None else lap_number
        
        details: EventDetails = {
            "lap_number": lap_number,
            "marker": track_limits_marker,
            "driver_roles": {f"{track_limits_driver_number}": "initiator"} if track_limits_driver_number is not None else None,
            "message": race_control_message
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
            category=EventCategory.DRIVER_ACTION.value,
            cause=EventCause.TRACK_LIMITS.value,
            details=details
        )


    def _process_incident_verdict(self, message: Message) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            date = message.timepoint

        # Lap number should be the current lap number since verdict is separate from incident
        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = self.lap_number
        
        # Extract incident verdict information from race control message
        # We need two patterns as penalty verdicts differ from others in structure
        incident_verdict_pattern = (
            r"^"
            r"(?:FIA\s+STEWARDS:\s+)?"
            r"(?:(?P<marker>[A-Z0-9/\s]+?)\s+)?"                                                        # Captures marker if it exists
            r"INCIDENT"
            r"(?:\s+INVOLVING\s+CARS?\s+(?P<driver_numbers>(?:\d+\s+\(\w+\)(?:\s*,\s*|\s+AND\s+)?)+))?" # Captures driver numbers if they exist
            r"\s+"
            r"(?P<verdict>[^-]+?)"                                                                      # Captures verdict
            r"(?:\s*-\s*(?P<reason>.+))?"                                                               # Captures reason if it exists
            r"$"
        )

        penalty_verdict_pattern = (
            r"^"
            r"(?:FIA\s+STEWARDS:\s+)?"
            r"(?P<verdict>.+?)"                                                                         # Captures verdict
            r"\s+FOR\s+CAR\s+"
            r"(?P<driver_number>\d+)\s+\(\w+\)"                                                         # Captures driver number
            r"(?:\s*-\s*(?P<reason>.+))?"                                                               # Captures reason if it exists
            r"$"
        )

        incident_verdict_match = re.search(pattern=incident_verdict_pattern, string=race_control_message)
        penalty_verdict_match = re.search(pattern=penalty_verdict_pattern, string=race_control_message)

        if incident_verdict_match is not None:
            # Standard incident verdict message
            incident_verdict_marker = str(incident_verdict_match.group("marker")) if incident_verdict_match.group("marker") is not None else None
            incident_verdict = str(incident_verdict_match.group("verdict")) if incident_verdict_match.group("verdict") is not None else None
            incident_verdict_reason = str(incident_verdict_match.group("reason")) if incident_verdict_match.group("reason") is not None else None
            
            try:
                incident_verdict_driver_numbers = [
                    int(driver_number) for driver_number in re.findall(r"(\d+)", str(incident_verdict_match.group("driver_numbers")))
                ]
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
                "lap_number": lap_number,
                "marker": incident_verdict_marker,
                "driver_roles": driver_roles,
                "verdict": incident_verdict,
                "reason": incident_verdict_reason,
                "message": race_control_message
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=date,
                elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
                category=EventCategory.DRIVER_NOTIFICATION.value,
                cause=EventCause.INCIDENT_VERDICT.value,
                details=details
            )
            
        elif penalty_verdict_match is not None:
            penalty_verdict = str(penalty_verdict_match.group("verdict")) if penalty_verdict_match.group("verdict") is not None else None
            penalty_verdict_reason = str(penalty_verdict_match.group("reason")) if penalty_verdict_match.group("reason") is not None else None
            penalty_verdict_driver_number = int(penalty_verdict_match.group("driver_number")) if penalty_verdict_match.group("driver_number") is not None else None
            
            details: EventDetails = {
                "lap_number": lap_number,
                "driver_roles": {f"{penalty_verdict_driver_number}": "initiator"} if penalty_verdict_driver_number is not None else None,
                "verdict": penalty_verdict,
                "reason": penalty_verdict_reason,
                "message": race_control_message
            }

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=date,
                elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
                category=EventCategory.DRIVER_NOTIFICATION.value,
                cause=EventCause.INCIDENT_VERDICT.value,
                details=details
            )

    
    def _process_provisional_classification(self, message: Message) -> Iterator[Event]:
        # Create provisional classification events for all drivers just before session end
        # Include personal best lap time for qualifying sessions only
        for driver_number, position in self.driver_positions.items():
            details: EventDetails = {
                "driver_roles": {f"{driver_number}": "initiator"},
                "position": position,
                "lap_duration": self.driver_personal_best_laps.get(driver_number) if self.session_type in ("Practice", "Qualifying") else None,
                "compound": self.driver_stints.get(driver_number, {}).get("compound") if self.session_type in ("Practice", "Qualifying") else None,
                "tyre_age_at_start": self.driver_stints.get(driver_number, {}).get("tyre_age_at_start") if self.session_type in ("Practice", "Qualifying") else None
            }

            # Adjust date so that higher positions are before lower positions when sorting chronologically by date
            # Since F1 only has millisecond precision timing this is inconsequential
            date = message.timepoint + timedelta(microseconds=position)

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=date,
                elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
                category=EventCategory.DRIVER_NOTIFICATION.value,
                cause=EventCause.PROVISIONAL_CLASSIFICATION.value,
                details=details
            )

        # Update session status with new session status
        self._update_session_status(message)

    
    def _process_qualifying_stage_classifications(self, message: Message) -> Iterator[Event]:
        # Determine whether drivers were eliminated or advanced from the previous qualifying stage
        try:
            current_qualifying_stage_number = int(deep_get(obj=message.content, key="SessionPart"))
        except:
            return
        
        if current_qualifying_stage_number not in (2, 3):
            return
        
        driver_status_data = deep_get(obj=message.content, key="Lines")

        if not isinstance(driver_status_data, dict):
            return
        
        for driver_number, data in driver_status_data.items():
            try:
                driver_number = int(driver_number)
            except:
                continue
            
            if not isinstance(data, dict):
                continue

            eliminated = bool(data.get("KnockedOut")) or False
            position = self.driver_positions.get(driver_number)

            details: EventDetails = {
                "driver_roles": {f"{driver_number}": "initiator"},
                "position": position,
                "lap_duration": self.driver_personal_best_laps.get(driver_number),
                "compound": self.driver_stints.get(driver_number, {}).get("compound"),
                "tyre_age_at_start": self.driver_stints.get(driver_number, {}).get("tyre_age_at_start"),
                # Advanced/eliminated from the previous stage
                "qualifying_stage_number": current_qualifying_stage_number - 1,
                "eliminated": eliminated
            }

            # Adjust date so that higher positions are before lower positions when sorting chronologically by date
            # Since F1 only has millisecond precision timing this is inconsequential
            date = message.timepoint + timedelta(microseconds=position)

            yield Event(
                meeting_key=self.meeting_key,
                session_key=self.session_key,
                date=date,
                elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
                category=EventCategory.DRIVER_NOTIFICATION.value,
                cause=EventCause.QUALIFYING_STAGE_CLASSIFICATION.value,
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
            date = message.timepoint

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = self.lap_number

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
            "driver_roles": {f"{driver_number}": "initiator"} if driver_number is not None else None,
            "message": race_control_message
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
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
            date = message.timepoint

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = self.lap_number
        
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
            elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
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
            date = message.timepoint

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = self.lap_number
        
        details: EventDetails = {
            "lap_number": lap_number,
            "message": race_control_message
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
            category=EventCategory.TRACK_NOTIFICATION.value,
            cause=event_cause.value,
            details=details
        )


    def _process_session_notification(self, message: Message, event_cause: EventCause) -> Iterator[Event]:
        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=message.timepoint,
            elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=message.timepoint),
            category=EventCategory.SESSION_NOTIFICATION.value,
            cause=event_cause.value,
            details=None
        )

        # Update session status with new session status for subsequent events
        self._update_session_status(message)


    def _process_race_control_message(self, message: Message) -> Iterator[Event]:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            date = message.timepoint

        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            lap_number = self.lap_number

        details: EventDetails = {
            "lap_number": lap_number,
            "message": race_control_message
        }

        yield Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=_get_elapsed_time(start=self.session_stream_start, end=date),
            category=EventCategory.OTHER.value,
            cause=EventCause.RACE_CONTROL_MESSAGE.value,
            details=details
        )


    # Maps event causes to unique conditions that determine if event messages belong to that cause
    # message should be of type Message
    def _get_event_condition_map(self) -> dict[EventCause, Callable[..., bool]]:
        return {
            EventCause.INCIDENT: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str),
                lambda: "INCIDENT" in deep_get(obj=message.content, key="Message"),
                lambda: "NOTED" in deep_get(obj=message.content, key="Message")
            ]),
            EventCause.OUT: lambda message: all(cond() for cond in [
                lambda: message.topic == "DriverRaceInfo",
                lambda: self.session_type == "Race",
                lambda: bool(deep_get(obj=message.content, key="IsOut")) is True
            ]),
            EventCause.OVERTAKE: lambda message: all(cond() for cond in [
                lambda: message.topic == "DriverRaceInfo",
                lambda: self.session_type == "Race",
                # Overtakes after the session has finished are likely penalties and should not be counted
                # but ignoring them affects the resulting sort order - might be caused by internal state updates resulting in a "lesser" hash
                lambda: self.session_status in ("Aborted", "Started"),
                lambda: deep_get(obj=message.content, key="OvertakeState") is not None,
                lambda: deep_get(obj=message.content, key="Position") is not None
            ]),
            EventCause.PERSONAL_BEST_LAP: lambda message: all(cond() for cond in [
                lambda: message.topic == "TimingData", 
                lambda: self.session_type in ("Practice", "Qualifying"),
                lambda: deep_get(obj=message.content, key="SessionPart") is None
            ]),
            EventCause.PIT: lambda message: all(cond() for cond in [
                lambda: message.topic == "TimingAppData",
                lambda: self.session_type == "Race",
                # Pit stops before the session has started should not be counted including pit stops on formation lap
                # but ignoring them affects the resulting sort order - also might be caused by internal state updates
                lambda: self.session_status in ("Aborted", "Started"),
                lambda: isinstance(deep_get(obj=message.content, key="Compound"), str)
            ]),
            EventCause.TRACK_LIMITS: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str),
                lambda: "TRACK LIMITS" in deep_get(obj=message.content, key="Message")
            ]),

            EventCause.BLACK_FLAG: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                # Check that message is a str to avoid TypeError when searching for substring
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str),
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
                # Handle edge cases
                lambda: "UNDER INVESTIGATION" not in deep_get(obj=message.content, key="Message"),
                lambda: "PENALTY SERVED" not in deep_get(obj=message.content, key="Message")
            ]),
            EventCause.PROVISIONAL_CLASSIFICATION: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_status is not None,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Finalised"
            ]),
            EventCause.QUALIFYING_STAGE_CLASSIFICATION: lambda message: all(cond() for cond in [
                lambda: message.topic == "TimingData", 
                lambda: self.session_type == "Qualifying",
                # We only know the results of the previous stage after the next stage begins
                lambda: deep_get(obj=message.content, key="SessionPart") in (2, 3)
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

            EventCause.SESSION_START: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_status is None,
                # First session status message always has empty series list
                lambda: isinstance(deep_get(obj=message.content, key="Series"), list),
                lambda: len(deep_get(obj=message.content, key="Series")) == 0
            ]),
            EventCause.SESSION_END: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_status is not None,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Ends"
            ]),
            EventCause.SESSION_STOP: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_status is not None,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Aborted"
            ]),
            EventCause.SESSION_RESUME: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_status == "Aborted", # Prev session status
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Started"
            ]),
            EventCause.PRACTICE_START: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Practice",
                lambda: self.session_status is None,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Started"
            ]),
            EventCause.PRACTICE_END: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Practice",
                lambda: self.session_status is not None,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Finished"
            ]),
            EventCause.Q1_START: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Qualifying",
                lambda: self.session_status is None,
                lambda: self.qualifying_stage_number == 1,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Started"
            ]),
            EventCause.Q1_END: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Qualifying",
                lambda: self.session_status is not None,
                lambda: self.qualifying_stage_number == 1,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Finished"
            ]),
            EventCause.Q2_START: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Qualifying",
                lambda: self.session_status == "Finished", # Prev session status
                lambda: self.qualifying_stage_number == 2,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Started"
            ]),
            EventCause.Q2_END: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Qualifying",
                lambda: self.session_status is not None,
                lambda: self.qualifying_stage_number == 2,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Finished"
            ]),
            EventCause.Q3_START: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Qualifying",
                lambda: self.session_status == "Finished", # Prev session status
                lambda: self.qualifying_stage_number == 3,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Started"
            ]),
            EventCause.Q3_END: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Qualifying",
                lambda: self.session_status is not None,
                lambda: self.qualifying_stage_number == 3,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Finished"
            ]),
            EventCause.RACE_START: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Race",
                lambda: self.session_status is None,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Started"
            ]),
            EventCause.RACE_END: lambda message: all(cond() for cond in [
                lambda: message.topic == "SessionData",
                lambda: self.session_type == "Race",
                lambda: self.session_status is not None,
                lambda: deep_get(obj=message.content, key="SessionStatus") == "Finished"
            ]),

            # Must be last since other events satisfy this condition
            EventCause.RACE_CONTROL_MESSAGE: lambda message: all(cond() for cond in [
                lambda: message.topic == "RaceControlMessages",
                lambda: isinstance(deep_get(obj=message.content, key="Message"), str)
            ])
        }

    
    # Maps event causes to specific processing logic
    # message should be of type Message
    def _get_event_processing_map(self) -> dict[EventCause, Callable[..., Iterator[Event]]]:
        return {
            EventCause.INCIDENT: lambda message: self._process_incident(message),
            EventCause.OUT: lambda message: self._process_outs(message),
            EventCause.OVERTAKE: lambda message: self._process_overtakes(message),
            EventCause.PERSONAL_BEST_LAP: lambda message: self._process_personal_best_laps(message),
            EventCause.PIT: lambda message: self._process_pits(message),
            EventCause.TRACK_LIMITS: lambda message: self._process_track_limits(message),
            
            EventCause.BLACK_FLAG: lambda message: self._process_driver_flag(message=message, event_cause=EventCause.BLACK_FLAG),
            EventCause.BLACK_AND_ORANGE_FLAG: lambda message: self._process_driver_flag(message=message, event_cause=EventCause.BLACK_AND_ORANGE_FLAG),
            EventCause.BLACK_AND_WHITE_FLAG: lambda message: self._process_driver_flag(message=message, event_cause=EventCause.BLACK_AND_WHITE_FLAG),
            EventCause.BLUE_FLAG: lambda message: self._process_driver_flag(message=message, event_cause=EventCause.BLUE_FLAG),
            EventCause.INCIDENT_VERDICT: lambda message: self._process_incident_verdict(message),
            EventCause.PROVISIONAL_CLASSIFICATION: lambda message: self._process_provisional_classification(message),
            EventCause.QUALIFYING_STAGE_CLASSIFICATION: lambda message: self._process_qualifying_stage_classifications(message),

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

            EventCause.SESSION_START: lambda message: self._process_session_notification(message=message, event_cause=EventCause.SESSION_START),
            EventCause.SESSION_END: lambda message: self._process_session_notification(message=message, event_cause=EventCause.SESSION_END),
            EventCause.SESSION_STOP: lambda message: self._process_session_notification(message=message, event_cause=EventCause.SESSION_STOP),
            EventCause.SESSION_RESUME: lambda message: self._process_session_notification(message=message, event_cause=EventCause.SESSION_RESUME),
            EventCause.PRACTICE_START: lambda message: self._process_session_notification(message=message, event_cause=EventCause.PRACTICE_START),
            EventCause.PRACTICE_END: lambda message: self._process_session_notification(message=message, event_cause=EventCause.PRACTICE_END),
            EventCause.Q1_START: lambda message: self._process_session_notification(message=message, event_cause=EventCause.Q1_START),
            EventCause.Q1_END: lambda message: self._process_session_notification(message=message, event_cause=EventCause.Q1_END),
            EventCause.Q2_START: lambda message: self._process_session_notification(message=message, event_cause=EventCause.Q2_START),
            EventCause.Q2_END: lambda message: self._process_session_notification(message=message, event_cause=EventCause.Q2_END),
            EventCause.Q3_START: lambda message: self._process_session_notification(message=message, event_cause=EventCause.Q3_START),
            EventCause.Q3_END: lambda message: self._process_session_notification(message=message, event_cause=EventCause.Q3_END),
            EventCause.RACE_START: lambda message: self._process_session_notification(message=message, event_cause=EventCause.RACE_START),
            EventCause.RACE_END: lambda message: self._process_session_notification(message=message, event_cause=EventCause.RACE_END),

            EventCause.RACE_CONTROL_MESSAGE: lambda message: self._process_race_control_message(message=message)
        }


    def process_message(self, message: Message) -> Iterator[Event]:
        match (message.topic):
            case "LapCount":
                self._update_lap_number(message)
            case "PitLaneTimeCollection":
                # Use "PitLaneTimeCollection" topic for sessions before the 2024 US GP weekend or for non-races afterward
                if self.session_type != "Race" or (self.session_start is not None and self.session_start < self.NEW_RACE_PIT_TOPIC_CUTOFF_DATE):
                    self._update_driver_pits(message)
            case "PitStopSeries":
                # Use "PitStopSeries" topic from the 2024 US GP afterward for races only
                if self.session_type == "Race" and (self.session_start is not None and self.session_start >= self.NEW_RACE_PIT_TOPIC_CUTOFF_DATE):
                    self._update_driver_pits(message)
            case "Position.z":
                self._update_driver_locations(message)
            case "SessionData":
                self._update_session_stream_start(message)
                if self.session_type == "Qualifying":
                    self._update_qualifying_stage_number(message)
            case "SessionInfo":
                self._update_session_info(message)
            case "TimingAppData":
                self._update_driver_stints(message)
            case "TimingData":
                self._update_driver_personal_best_laps(message)
                self._update_driver_positions(message)
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

        