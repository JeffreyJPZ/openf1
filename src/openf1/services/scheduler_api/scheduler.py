from datetime import datetime, timedelta, timezone
import os
import requests

import typer

from openf1.services.f1_scraping.schedule import get_meetings, ingest_meetings, ingest_sessions
from openf1.services.scheduler_api.app import IngestJob, IngestJobData, IngestService

dapr_host = os.getenv('DAPR_HOST', 'http://dapr-openf1-api-historical-processing')
dapr_app_id = os.getenv('DAPR_APP_ID', 'openf1-historical-processing')
dapr_port = os.getenv('DAPR_HTTP_PORT', '3500')

app = typer.Typer()


def schedule_job(job: IngestJob) -> None:

    print(f"Sending request to schedule job: {job.name}", flush=True)

    try:
        # Use HTTP client to call the scheduler service via Dapr
        req_url = f"{dapr_host}:{dapr_port}/v1.0/invoke/{dapr_app_id}/method/scheduleJob"

        response = requests.post(
            req_url,
            json=job.model_dump(),
            headers={"Content-Type": "application/json"},
            timeout=15
        )

        # Accept both 200 and 204 as success codes
        if response.status_code not in [200, 204]:
            raise Exception(
                f"Failed to schedule job. Status code: {response.status_code}, Response: {response.text}", flush=True)

        if response.text:
            print(f"Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"Error scheduling job {job.name}: {str(e)}", flush=True)
        raise


@app.command()
def get_job_details(name: str) -> None:

    print(f"Sending request to retrieve job: {name}", flush=True)

    try:
        # Use HTTP client to call the scheduler service via Dapr
        req_url = f"{dapr_host}:{dapr_port}/v1.0/invoke/{dapr_app_id}/method/getJob/{name}"

        response = requests.get(req_url)

        if response.status_code in [200, 204]:
            print(f"Job details for {name}: {response.text}", flush=True)
        else:
            print(f"Failed to get job details. Status code: {response.status_code}, Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"Error getting job details for {name}: {str(e)}", flush=True)
        raise


@app.command()
def schedule_season_ingestion(year: int, collection_names: list[str]):
    """
    Schedules ingestion of all historical data for the given collections in a season.
    A meeting is scheduled to be ingested later if the current date is less than the meeting end date + some delay (need to wait for upstream F1 source to update),
    otherwise it is scheduled to be ingested immediately.
    """
    # Ingest meeting and session info from the `f1_scraping` schedule service if given
    if "meetings" in collection_names:
        ingest_meetings(year)
    if "sessions" in collection_names:
        ingest_sessions(year)

    meetings = get_meetings(year)

    DELAY_HRS = 6
    for meeting in meetings:
        try:
            curr_date = datetime.now(timezone.utc)

            # Meeting dates are in UTC
            threshold_date = meeting.date_end + timedelta(hours=DELAY_HRS)

            if curr_date < threshold_date:
                due_time = threshold_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                due_time = "0s"

            schedule_job(
                IngestJob(
                    name=f"ingest_meeting_{meeting.meeting_key}",
                    data=IngestJobData(
                        service=IngestService.INGEST_MEETING,
                        year=meeting.year,
                        meeting_key=meeting.meeting_key,
                        collection_names=collection_names,
                    ),
                    due_time=due_time,
                )
            )

            print(f"Sucessfully scheduled meeting {meeting.meeting_key}")
        except Exception:
            print(f"Failed to schedule meeting {meeting.meeting_key}")


if __name__ == "__main__":
    app()