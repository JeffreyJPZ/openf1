import re
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
    DRIVER_ACTION = "driver-action" # Actions by drivers - pits, outs, overtakes, track limits violations, incidents
    DRIVER_NOTIFICATION = "driver-notification" # Race control messsages to drivers - blue flags, black flags, black and white flags, black and orange flags
    SECTOR_NOTIFICATION = "sector-notification" # Green (sector clear), yellow, double-yellow flags
    TRACK_NOTIFICATION = "track-notification" # Green (track clear) flags, red flags, safety cars
    INCIDENT_NOTIFICATION = "incident-notification" # Incident verdicts by stewards
    OTHER = "other"


class EventCause(str, Enum):
    PIT = "pit"
    OUT = "out"
    OVERTAKE = "overtake"
    OFF_TRACK = "off-track" # Track limits violations
    INCIDENT = "incident" # Collisions, unsafe rejoin, safety car/start infringements, etc.
    SAFETY_CAR_DEPLOYED = "safety-car-deployed"
    VIRTUAL_SAFETY_CAR_DEPLOYED = "virtual-safety-car-deployed"
    SAFETY_CAR_ENDING = "safety-car-ending"
    VIRTUAL_SAFETY_CAR_ENDING = "virtual-safety-car-ending"
    GREEN_FLAG = "green-flag"
    YELLOW_FLAG = "yellow-flag"
    DOUBLE_YELLOW_FLAG = "double-yellow-flag"
    RED_FLAG = "red-flag"
    BLUE_FLAG = "blue-flag"
    BLACK_AND_WHITE_FLAG = "black-and-white-flag"
    BLACK_AND_ORANGE_FLAG = "black-and-orange-flag"
    BLACK_FLAG = "black-flag"
    CHEQUERED_FLAG = "chequered-flag"


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
        "SessionInfo"
    }
    
    # Since messages are sorted by timepoint and then by topic we only need to keep the most recent data from other topics?
    session_date_start: datetime = field(default=None)
    session_type: str = field(default=None)
    current_lap_number: int = field(default=None)
    driver_positions: dict[int, dict[Literal["X"] | Literal["Y"] | Literal["Z"], int]] = field(default_factory=dict)
    

    def _update_driver_position(self, driver_number: int, property: Literal["X"] | Literal["Y"] | Literal["Z"], value: int):
        driver_position = self.driver_positions[driver_number]
        old_value = getattr(driver_position, property)
        if value != old_value:
            setattr(driver_position, property, value)
    

    def _process_pit(self, message: Message) -> Event | None:
        pit_data = message.content.get("PitTimes")

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
            "lap_number": self.current_lap_number,
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
            "lap_number": self.current_lap_number,
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
            "lap_number": self.current_lap_number,
            "driver_roles": driver_roles,
            "location_x": self.driver_positions.get(overtaking_driver_number).get("X"),
            "location_y": self.driver_positions.get(overtaking_driver_number).get("Y"),
            "location_z": self.driver_positions.get(overtaking_driver_number).get("Z")
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
    

    def _process_off_track(self, message: Message) -> Event | None:
        race_control_message = deep_get(obj=message.content, key="Message")

        if not isinstance(race_control_message, str):
            return
        
        # Find driver number, turn number, lap number, and UTC time of track limit violation
        try:
            match = re.search(
                r"CAR\s+(?P<driver_number>\d+).*?TURN\s+(?P<turn_number>\d+).*?LAP\s+(?P<lap_number>\d+)\s+(?P<time>\b\d{2}:\d{2}:\d{2}\b)",
                race_control_message
            )
            driver_number = int(match.group("driver_number"))
            turn_number = int(match.group("turn_number"))
            lap_number = int(match.group("lap_number"))
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
            elapsed_time=message.timepoint - self.session_date_start,
            category=EventCategory.DRIVER_ACTION,
            cause=EventCause.OFF_TRACK,
            details=details
        )
    

    def _process_sector_flag(self, message: Message, event_cause: EventCause) -> Event | None:
        race_control_message = deep_get(obj=message.content, key="Message")

        # Turn numbers are referred to as "sectors" for some reason
        try:
            turn_number = int(deep_get(obj=message.content, key="Sector"))
        except:
            return
        
        try:
            lap_number = int(deep_get(obj=message.content, key="Lap"))
        except:
            return
        
        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            return
        
        details = {
            "lap_number": lap_number,
            "turn_number": turn_number,
            "message": race_control_message,
            "driver_roles": None # Include null fields?
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=message.timepoint - self.session_date_start,
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
        except:
            return
        
        try:
            date = to_datetime(deep_get(obj=message.content, key="Utc"))
            date = pytz.utc.localize(date)
        except:
            return
        
        # Turn numbers are referred to as "track sectors" for some reason
        try:
            match = re.search(r"TRACK\s+SECTOR\s+(?P<turn_number>\d+)", race_control_message)
            turn_number = match.group("turn_number")
        except:
            return
        
        details = {
            "lap_number": lap_number,
            "turn_number": turn_number,
            "message": race_control_message,
            "driver_roles": None # Include null fields?
        }

        return Event(
            meeting_key=self.meeting_key,
            session_key=self.session_key,
            date=date,
            elapsed_time=message.timepoint - self.session_date_start,
            category=EventCategory.TRACK_NOTIFICATION,
            cause=event_cause,
            details=details
        )


    # Maps event causes to unique conditions that determine if event messages belong to that cause
    # data should be of type dict
    def _get_event_condition_map(self) -> dict[EventCause, Callable[..., bool]]:
        return {
            EventCause.PIT: lambda data: deep_get(obj=data, key="Duration") is not None,
            EventCause.OUT: lambda data: deep_get(obj=data, key="IsOut") is not None,
            EventCause.OVERTAKE: lambda data: deep_get(obj=data, key="OvertakeState") is not None and deep_get(obj=data, key="Position") is not None,
            EventCause.OFF_TRACK: lambda data: deep_get(obj=data, key="Category") == "Other" and "TRACK LIMITS" in deep_get(obj=data, key="Message"),
            EventCause.INCIDENT: lambda data: deep_get(obj=data, key="Category") == "Other" and "INCIDENT" in deep_get(obj=data, key="Message"),
            EventCause.SAFETY_CAR_DEPLOYED: lambda data: deep_get(obj=data, key="Category") == "SafetyCar" and deep_get(obj=data, key="Mode") == "SAFETY CAR",
            EventCause.VIRTUAL_SAFETY_CAR_DEPLOYED: lambda data: deep_get(obj=data, key="Category") == "SafetyCar" and deep_get(obj=data, key="Mode") == "VIRTUAL SAFETY CAR",
            EventCause.GREEN_FLAG: lambda data: "GREEN" or "CLEAR" in deep_get(obj=data, key="Message"),
            EventCause.YELLOW_FLAG: lambda data: "YELLOW" in deep_get(obj=data, key="Message"),
            EventCause.DOUBLE_YELLOW_FLAG: lambda data: "DOUBLE YELLOW" in deep_get(obj=data, key="Message"),
            EventCause.RED_FLAG: lambda data: "RED" in deep_get(obj=data, key="Message"),
            EventCause.BLUE_FLAG: lambda data: "BLUE" in deep_get(obj=data, key="Message"),
            EventCause.BLACK_AND_WHITE_FLAG: lambda data: "BLACK AND WHITE" in deep_get(obj=data, key="Message"),
            EventCause.BLACK_AND_ORANGE_FLAG: lambda data: "BLACK AND ORANGE" in deep_get(obj=data, key="Message"),
            EventCause.BLACK_FLAG: lambda data: "BLACK" in deep_get(obj=data, key="Message"),
            EventCause.CHEQUERED_FLAG: lambda data: "CHEQUERED" in deep_get(obj=data, key="Message")
        }

    # Maps event causes to specific processing logic
    # message should be of type Message
    def _get_event_processing_map(self) -> dict[EventCause, Callable[..., Event | None]]:
        return {
            EventCause.PIT: lambda message: self._process_pit(message),
            EventCause.OUT: lambda message: self._process_out(message),
            EventCause.OVERTAKE: lambda message: self._process_overtake(message),
            EventCause.OFF_TRACK: lambda message: self._process_off_track(message),
            EventCause.GREEN_FLAG: lambda message: 
                self._process_sector_flag(message=message, event_cause=EventCause.GREEN_FLAG) 
                if deep_get(obj=message.content, key="Scope") == "Sector"
                else self._process_track_flag(message=message, event_cause=EventCause.GREEN_FLAG),
            EventCause.YELLOW_FLAG: lambda message: self._process_sector_flag(message=message, event_cause=EventCause.YELLOW_FLAG),
            EventCause.DOUBLE_YELLOW_FLAG: lambda message: self._process_sector_flag(message=message, event_cause=EventCause.DOUBLE_YELLOW_FLAG)
            # TODO: add remaining event causes
        }


    def process_message(self, message: Message) -> Iterator[Event]:
        if message.topic == "LapCount":
            # Update lap number
            try:
                current_lap_number = int(message.content.get("CurrentLap"))
            except:
                return
            
            self.current_lap_number = current_lap_number

        elif message.topic == "SessionInfo":
            # Update session date and type
            try:
                date_start = to_datetime(message.content.get("StartDate"))
                gmt_offset = message.content.get("GmtOffset")
                date_start = add_timezone_info(dt=date_start, gmt_offset=gmt_offset)
            except:
                return

            try:
                session_type = str(message.content.get("Type"))
            except:
                return
            
            self.session_date_start = date_start
            self.session_type = session_type

        elif message.topic == "Position.z":
            # Update driver positions
            positions = message.content.get("Position")

            if not isinstance(positions, list):
                return
            
            latest_positions = positions[-1]

            if not isinstance(latest_positions, dict):
                return
            
            latest_entries = latest_positions.get("Entries")

            if not isinstance(latest_entries, dict):
                return
            
            for driver_number, data in latest_entries.items():
                try:
                    driver_number = int(driver_number)
                except:
                    continue

                if not isinstance(data, dict):
                    continue
                
                for property, value in data.items():
                    if property == "Status":
                        continue

                    self._update_driver_position(
                        driver_number=driver_number,
                        property=property,
                        value=value
                    )

        else:
            # Find event cause corresponding to message
            event_cause = next(
                (event_cause for event_cause, condition in self._get_event_condition_map().items()
                    if condition(message.content)
                ),
                None
            )

            if event_cause is None:
                # Not an event message
                return
            
            yield self._get_event_processing_map()[event_cause](message)

        