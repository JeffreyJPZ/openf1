import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Iterator, Literal

import pytz

from openf1.services.ingestor_livetiming.core.objects import (
    Collection,
    Document,
    Message,
)
from openf1.util.misc import to_datetime, add_timezone_info


def deep_get(obj: Any, key: Any) -> Any:
    """
    This function was adapted from https://stackoverflow.com/a/9808122.
    Returns the first value indexed by the given key in an arbitrarily nested dictionary.
    otherwise returns None.
    """
    for k, v in (
        obj.items() if isinstance(obj, dict) else
        enumerate(obj) if isinstance(obj, list) else
        []
    ):
        if k == key:
            return v
        elif isinstance(v, (dict, list)):
            return deep_get(v, key)
        
    return False
    

class EventCategory(str, Enum):
    DRIVER_ACTION = "driver-action" # Actions by drivers - pits, outs, overtakes, hotlaps, track limits violations, incidents
    DRIVER_NOTIFICATION = "driver-notification" # Race control messsages to drivers - blue flags, black flags, black and white flags, black and orange flags, incident verdicts
    SECTOR_NOTIFICATION = "sector-notification" # Green (sector clear), yellow, double-yellow flags
    TRACK_NOTIFICATION = "track-notification" # Green (track clear) flags, red flags, chequered flags, safety cars, start of qualifying session parts


class EventCause(str, Enum):
    # Driver actions
    HOTLAP = "hotlap" # Used in qualifying/practice sessions
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
    

@dataclass(eq=False)
class Event(Document):
    meeting_key: int
    session_key: int
    date: datetime
    elapsed_time: timedelta
    category: str
    cause: str
    details: dict[str, Any]

    @property
    def unique_key(self) -> tuple:
        return (self.date, self.cause)


class EventsCollection(Collection):
    name = "events"
    source_topics = {
        "DriverRaceInfo",
        "LapCount",
        "PitLaneTimeCollection",
        "Position.z",
        "RaceControlMessages",
        "SessionInfo",
        "TimingData",
        "TimingAppData"
    }
    
    # Since messages are sorted by timepoint and then by topic we only need to keep the most recent data from other topics?
    session_date_start: datetime = field(default=None)
    session_type: str = field(default=None)
    lap_number: int = field(default=None)
    driver_positions: dict[int, dict[Literal["x", "y", "z"], int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))

    # Combine latest stint data with latest pit data for pit event - stint number should be one more than pit number
    driver_stints: dict[int, dict[Literal["stint_number", "compound", "is_new", "tyre_age_at_start"], bool | int | str]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))
    driver_pits: dict[int, dict[Literal["date", "pit_duration", "lap_number"], datetime | float | int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))


    def _update_lap_number(self, message: Message):
        # Update current lap number
            try:
                lap_number = int(deep_get(obj=message.content, key="CurrentLap"))
            except:
                return
            
            self.lap_number = lap_number


    def _update_session_info(self, message: Message):
        # Update session start date and type
        try:
            date_start = to_datetime(deep_get(obj=message.content, key="StartDate"))
            gmt_offset = deep_get(obj=message.content, key="GmtOffset")
            date_start = add_timezone_info(dt=date_start, gmt_offset=gmt_offset)

            session_type = str(deep_get(obj=message.content, key="Type"))
        except:
            return
        
        self.session_date_start = date_start
        self.session_type = session_type


    def _update_driver_position(self, driver_number: int, property: Literal["x", "y", "z"], value: int):
        driver_position = self.driver_positions.get(driver_number)
        old_value = getattr(driver_position, property, None)
        if value != old_value:
            setattr(driver_position, property, value)
            

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
                property="x",
                value=x
            )
            self._update_driver_position(
                driver_number=driver_number,
                property="y",
                value=y
            )
            self._update_driver_position(
                driver_number=driver_number,
                property="z",
                value=z
            )

                
    def _update_driver_stint(self, driver_number: int, property: Literal["stint_number", "compound", "is_new", "tyre_age_at_start"], value: bool | int | str):
        driver_stint = self.driver_stints.get(driver_number)
        old_value = getattr(driver_stint, property, None)
        if value != old_value:
            setattr(driver_stint, property, value)
            

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

            if not isinstance(driver_stints, dict) or len(driver_stints.keys() == 0):
                continue
            
            latest_stint_number = max(driver_stints.keys(), key=lambda stint_number: int(stint_number))
            latest_stint_data = deep_get(obj=driver_stints, key=latest_stint_number)

            if not isinstance(latest_stint_data, dict):
                continue

            # Stint numbers are 0-indexed, convert to 1-indexing
            try:
                latest_stint_number = int(latest_stint_number) + 1
                compound = str(latest_stint_data.get("Compound"))
                is_new = True if str(latest_stint_data.get("New")) == "true" else False
                total_laps = int(latest_stint_data.get("TotalLaps"))
            except:
                continue

            self._update_driver_stint(
                driver_number=driver_number,
                property="stint_number",
                value=latest_stint_number
            )
            self._update_driver_stint(
                driver_number=driver_number,
                property="compound",
                value=compound
            )
            self._update_driver_stint(
                driver_number=driver_number,
                property="is_new",
                value=is_new
            )
            self._update_driver_stint(
                driver_number=driver_number,
                property="tyre_age_at_start",
                value=total_laps
            )
        

    def _update_driver_pit(self, driver_number: int, property: Literal["date", "pit_duration", "lap_number"], value: datetime | float | int):
        driver_pit = self.driver_pits.get(driver_number)
        old_value = getattr(driver_pit, property, None)
        if value != old_value:
            setattr(driver_pit, property, value)


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
            
            date = message.timepoint

            self._update_driver_pit(
                driver_number=driver_number,
                property="date",
                value=date
            )
            self._update_driver_pit(
                driver_number=driver_number,
                property="pit_duration",
                value=pit_duration
            )
            self._update_driver_pit(
                driver_number=driver_number,
                property="lap_number",
                value=lap_number
            )


    def _process_hotlap(self, message: Message) -> Event | None:
        return
    

    def _process_incident(self, message: Message) -> Event | None:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            return
        
        # Find driver number(s), turn number (if it exists), lap number (if it exists), and incident reason (if it exists) for incident
        try:
            pattern = re.compile(
                r"(?:TURN (?P<turn_number>\d+)\s+)?"                                                        # Captures turn number if it exists
                r"(?:LAP\s+(?P<lap_number>\d+)\s+)?"                                                        # Captures lap number if it exists
                r"INCIDENT"
                r"(?:\s+INVOLVING\s+CARS?\s+(?P<driver_numbers>(?:\d+\s+\(\w+\)(?:\s*,\s*|\s+AND\s+)?)+))?" # Captures driver numbers if they exist
                r"\s+NOTED(?:\s+-\s+(?P<incident_reason>.+))?"                                              # Captures incident reason if it exists
            )
            match = pattern.search(race_control_message)

            lap_number = int(match.group("lap_number")) if match.group("lap_number") is not None else None
            turn_number = int(match.group("turn_number")) if match.group("turn_number") is not None else None
            driver_numbers = str(match.group("driver_numbers")) if match.group("driver_numbers") is not None else None
            driver_numbers = [int(driver_number) for driver_number in re.findall(r"(\d+)", driver_numbers)] if driver_numbers is not None else None
            incident_reason = str(match.group("incident_reason")).replace(" ", "-").lower() if match.group("incident_reason") is not None else None
        except:
            return
        
        # Assume incidents with turn number are between drivers and have driver at fault listed first,
        # and that incidents between more than two drivers do not specify the exact drivers (i.e. driver_numbers is None)
        if driver_numbers is None:
            # Incident does not specify drivers
            driver_roles = None
        elif turn_number is not None:
            # Incident is between two drivers, with the first listed driver at fault
            # TODO: more exception handling
            initiator_driver_number = driver_numbers[0]
            participant_driver_number = driver_numbers[1]

            driver_roles = {
                initiator_driver_number: "initiator",
                participant_driver_number: "participant"
            }
        else:
            # Incident is not between drivers
            driver_roles = {driver_number: "initiator" for driver_number in driver_numbers}

        details = {
            "lap_number": lap_number,
            "turn_number": turn_number,
            "reason": incident_reason,
            "message": race_control_message,
            "driver_roles": driver_roles
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=date - self.session_date_start,
            category=EventCategory.DRIVER_ACTION,
            cause=EventCause.INCIDENT,
            details=details
        )
    

    def _process_off_track(self, message: Message) -> Event | None:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        # Find driver number, turn number, lap number, and UTC time for track limit violation
        try:
            pattern = re.compile(
                r"CAR\s+(?P<driver_number>\d+).*?"  # Captures driver number
                r"TURN\s+(?P<turn_number>\d+).*?"   # Captures turn number
                r"LAP\s+(?P<lap_number>\d+)\s+"     # Captures lap number
                r"(?P<time>\b\d{2}:\d{2}:\d{2}\b)"  # Captures UTC time
            )
            match = pattern.search(race_control_message)

            driver_number = int(match.group("driver_number"))
            lap_number = int(match.group("lap_number"))
            turn_number = int(match.group("turn_number"))
            time = str(match.group("time"))

            # Combine UTC time with session date to get accurate time of track limit violation
            date = self.session_date_start.combine(
                date=self.session_date_start.date(),
                time=datetime.strptime(time, "%H:%M:%S").time(),
                tzinfo=self.session_date_start.tzinfo
            )
        except:
            return
        
        details = {
            "lap_number": lap_number,
            "turn_number": turn_number,
            "driver_roles": {driver_number: "initiator"},
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=date - self.session_date_start,
            category=EventCategory.DRIVER_ACTION,
            cause=EventCause.OFF_TRACK,
            details=details
        )
    

    def _process_out(self, message: Message) -> Event | None:
        driver_number = next(
            (driver_number for driver_number, data in message.content.items()
                if isinstance(driver_number, int) and isinstance(data, dict) and data.get("IsOut") == True
            ),
            None
        )

        if driver_number is None:
            return

        details = {
            "lap_number": self.lap_number,
            "driver_roles": {driver_number: "initiator"},
            "location_x": self.driver_positions.get(driver_number).get("x"),
            "location_y": self.driver_positions.get(driver_number).get("y"),
            "location_z": self.driver_positions.get(driver_number).get("z")
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=message.timepoint,
            elapsed_time=message.timepoint - self.session_date_start,
            category=EventCategory.DRIVER_ACTION,
            cause=EventCause.OUT,
            details=details
        )
        

    def _process_overtake(self, message: Message) -> Event | None:
        # Separate overtaking driver from overtaken drivers
        # Overtake state 2 indicates that the driver is the one overtaking, all other drivers are being overtaken
        overtaking_driver_number = next(
            (driver_number for driver_number, data in message.content.items()
                if isinstance(driver_number, int) and isinstance(data, dict) and data.get("OvertakeState") == 2
            ),
            None
        )
        overtaken_driver_numbers = [driver_number for driver_number, data in message.content.items()
            if isinstance(driver_number, int) and isinstance(data, dict) and data.get("OvertakeState") != 2
        ]

        if overtaking_driver_number is None or not overtaken_driver_numbers:
            return
        
        # Create and merge driver roles
        driver_roles = {
            **{overtaking_driver_number: "initiator"},
            **{driver_number: "participant" for driver_number in overtaken_driver_numbers}
        }
        
        details = {
            "lap_number": self.lap_number,
            "driver_roles": driver_roles,
            "location_x": self.driver_positions.get(overtaking_driver_number).get("x"),
            "location_y": self.driver_positions.get(overtaking_driver_number).get("y"),
            "location_z": self.driver_positions.get(overtaking_driver_number).get("z")
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=message.timepoint,
            elapsed_time=message.timepoint - self.session_date_start,
            category=EventCategory.DRIVER_ACTION,
            cause=EventCause.OVERTAKE,
            details=details
        )
    

    def _process_pit(self, message: Message) -> Event | None:
        # TODO: check if session is race and 
        pit_data = deep_get(obj=message.content, key="PitTimes")

        if not isinstance(pit_data, dict):
            return
        
        driver_number = next(
            (driver_number for driver_number, data in pit_data.items()
                if isinstance(driver_number, int) and isinstance(data, dict)
            ),
            None
        )

        if driver_number is None:
            return
        
        details = {
            "lap_number": self.lap_number,
            "driver_roles": {driver_number: "initiator"},
            "location_x": self.driver_positions.get(driver_number).get("X"),
            "location_y": self.driver_positions.get(driver_number).get("Y"),
            "location_z": self.driver_positions.get(driver_number).get("Z")
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=message.timepoint,
            elapsed_time=message.timepoint - self.session_date_start,
            category=EventCategory.DRIVER_ACTION,
            cause=EventCause.PIT,
            details=details
        )
    

    def _process_incident_verdict(self, message: Message) -> Event | None:
        return
    

    def _process_driver_flag(self, message: Message, event_cause: EventCause) -> Event | None:
        race_control_message = deep_get(obj=message.content, key="Message")

        try:
            driver_number = int(deep_get(obj=message.content, key="RacingNumber"))
            lap_number = int(deep_get(obj=message.content, key="Lap")) 
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            return
        
        details = {
            "lap_number": lap_number,
            "message": race_control_message,
            "driver_roles": {driver_number: "initiator"}
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=date - self.session_date_start,
            category=EventCategory.DRIVER_NOTIFICATION,
            cause=event_cause,
            details=details
        )
    

    def _process_sector_flag(self, message: Message, event_cause: EventCause) -> Event | None:
        race_control_message = deep_get(obj=message.content, key="Message")

        # Turn numbers are referred to as "sectors" for some reason
        try:
            turn_number = int(deep_get(obj=message.content, key="Sector"))
            lap_number = int(deep_get(obj=message.content, key="Lap"))
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            return
        
        details = {
            "lap_number": lap_number,
            "turn_number": turn_number,
            "message": race_control_message
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=date - self.session_date_start,
            category=EventCategory.SECTOR_NOTIFICATION,
            cause=event_cause,
            details=details
        )
    

    def _process_track_flag(self, message: Message, event_cause: EventCause) -> Event | None:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            return
        
        details = {
            "lap_number": lap_number,
            "message": race_control_message
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=date - self.session_date_start,
            category=EventCategory.TRACK_NOTIFICATION,
            cause=event_cause,
            details=details
        )
    

    def _process_qualifying_part_start(self, message: Message, event_cause: EventCause) -> Event | None:
        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=message.timepoint,
            elapsed_time=message.timepoint - self.session_date_start,
            category=EventCategory.TRACK_NOTIFICATION,
            cause=event_cause,
            details=None
        )


    # Maps event causes to unique conditions that determine if event messages belong to that cause
    # message should be of type Message
    def _get_event_condition_map(self) -> dict[EventCause, Callable[..., bool]]:
        return {
            EventCause.HOTLAP: lambda message: True, # TODO: determine fields that identify a hotlap
            EventCause.INCIDENT: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Message") is not None and all([
                "INCIDENT" in deep_get(obj=message.content, key="Message"),
                "NOTED" in deep_get(obj=message.content, key="Message")
            ]),
            EventCause.OFF_TRACK: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Message") is not None and "TRACK LIMITS" in deep_get(obj=message.content, key="Message"),
            EventCause.OUT: lambda message: message.topic == "DriverRaceInfo" and deep_get(obj=message.content, key="IsOut") is not None,
            EventCause.OVERTAKE: lambda message: message.topic == "DriverRaceInfo" and deep_get(obj=message.content, key="OvertakeState") is not None and deep_get(obj=message.content, key="Position") is not None,
            EventCause.PIT: lambda message: True, # TODO: use stint fields from TimingAppData to create pit event since they are behind by several seconds

            EventCause.BLACK_FLAG: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Message") is not None and "BLACK" in deep_get(obj=message.content, key="Message"), # Black flags do not have their own category
            EventCause.BLACK_AND_ORANGE_FLAG: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Message") is not None and "BLACK AND ORANGE" in deep_get(obj=message.content, key="Message"),
            EventCause.BLACK_AND_WHITE_FLAG: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Message") is not None and "BLACK AND WHITE" in deep_get(obj=message.content, key="Message"),
            EventCause.BLUE_FLAG: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Flag") is not None and deep_get(obj=message.content, key="Flag") == "BLUE",
            EventCause.INCIDENT_VERDICT: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Message") is not None and all([
                "FIA STEWARDS" in deep_get(obj=message.content, key="Message"),
                "UNDER INVESTIGATION" not in deep_get(obj=message.content, key="Message") # "Under investigation" is not a verdict
            ]),

            EventCause.GREEN_FLAG: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Flag") is not None and any([
                deep_get(obj=message.content, key="Flag") == "GREEN",
                deep_get(obj=message.content, key="Flag") == "CLEAR",
            ]),
            EventCause.YELLOW_FLAG: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Flag") is not None and deep_get(obj=message.content, key="Flag") == "YELLOW",
            EventCause.DOUBLE_YELLOW_FLAG: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Flag") is not None and deep_get(obj=message.content, key="Flag") == "DOUBLE YELLOW",
            
            EventCause.CHEQUERED_FLAG: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Flag") is not None and deep_get(obj=message.content, key="Flag") == "CHEQUERED",
            EventCause.RED_FLAG: lambda message: message.topic == "RaceControlMessages" and deep_get(obj=message.content, key="Flag") is not None and deep_get(obj=message.content, key="Flag") == "RED",
            EventCause.SAFETY_CAR_DEPLOYED: lambda message: message.topic == "RaceControlMessages" and all([
                deep_get(obj=message.content, key="Category") is not None and deep_get(obj=message.content, key="Category") == "SafetyCar",
                deep_get(obj=message.content, key="Mode") is not None and deep_get(obj=message.content, key="Mode") == "SAFETY CAR",
                deep_get(obj=message.content, key="Status") is not None and deep_get(obj=message.content, key="Status") == "DEPLOYED"
            ]),
            EventCause.VIRTUAL_SAFETY_CAR_DEPLOYED: lambda message: message.topic == "RaceControlMessages" and all([
                deep_get(obj=message.content, key="Category") is not None and deep_get(obj=message.content, key="Category") == "SafetyCar",
                deep_get(obj=message.content, key="Mode") is not None and deep_get(obj=message.content, key="Mode") == "VIRTUAL SAFETY CAR",
                deep_get(obj=message.content, key="Status") is not None and deep_get(obj=message.content, key="Status") == "DEPLOYED"
            ]),
            EventCause.SAFETY_CAR_ENDING: lambda message: message.topic == "RaceControlMessages" and all([
                deep_get(obj=message.content, key="Category") is not None and deep_get(obj=message.content, key="Category") == "SafetyCar",
                deep_get(obj=message.content, key="Mode") is not None and deep_get(obj=message.content, key="Mode") == "SAFETY CAR",
                deep_get(obj=message.content, key="Status") is not None and deep_get(obj=message.content, key="Status") == "IN THIS LAP"
            ]),
            EventCause.VIRTUAL_SAFETY_CAR_ENDING: lambda message: message.topic == "RaceControlMessages" and all([
                deep_get(obj=message.content, key="Category") is not None and deep_get(obj=message.content, key="Category") == "SafetyCar",
                deep_get(obj=message.content, key="Mode") is not None and deep_get(obj=message.content, key="Mode") == "VIRTUAL SAFETY CAR",
                deep_get(obj=message.content, key="Status") is not None and deep_get(obj=message.content, key="Status") == "ENDING"
            ]),
            EventCause.Q1_START: lambda message: message.topic == "TimingData" and deep_get(obj=message.content, key="SessionPart") is not None and deep_get(obj=message.content, key="SessionPart") == 1, 
            EventCause.Q2_START: lambda message: message.topic == "TimingData" and deep_get(obj=message.content, key="SessionPart") is not None and deep_get(obj=message.content, key="SessionPart") == 2,
            EventCause.Q3_START: lambda message: message.topic == "TimingData" and deep_get(obj=message.content, key="SessionPart") is not None and deep_get(obj=message.content, key="SessionPart") == 3
        }

    # Maps event causes to specific processing logic
    # message should be of type Message
    def _get_event_processing_map(self) -> dict[EventCause, Callable[..., Event | None]]:
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

            EventCause.GREEN_FLAG: lambda message: 
                self._process_sector_flag(message=message, event_cause=EventCause.GREEN_FLAG) 
                if deep_get(obj=message.content, key="Scope") is not None and deep_get(obj=message.content, key="Scope") == "Sector"
                else self._process_track_flag(message=message, event_cause=EventCause.GREEN_FLAG),
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
        if message.topic == "LapCount":
            self._update_lap_number(message)
        elif message.topic == "PitLaneTimeCollection":
            self._update_driver_pits(message)
        elif message.topic == "Position.z":
            self._update_driver_positions(message)
        elif message.topic == "SessionInfo":
            self._update_session_info(message)
        elif message.topic == "TimingAppData":
            self._update_driver_stints(message)
        
        # Find event cause corresponding to message
        event_cause = next(
            (event_cause for event_cause, condition in self._get_event_condition_map().items()
                if condition(message.content)
            ),
            None
        )

        if event_cause is None:
            # Not an event message
            yield None
        
        yield self._get_event_processing_map().get(event_cause)(message)       

        