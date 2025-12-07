import sys
import subprocess
from prefect import flow, task, logger

@task
def run_script(script_path, flags):
    command = [sys.executable, "-m", script_path] + flags

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in iter(process.stdout.readline, ""):
        print(line, end="")

    process.wait()

    if process.returncode != 0:
        raise Exception(f"Script {script_path} failed with code {process.returncode}")

@flow(name="ingestion_flow", log_prints=True)
def ingestion_flow(page_number: int, batch_number: int):
    p = str(page_number)
    b = str(batch_number)

    run_script("ingestion.get_artists", ["--page_number", p, "--batch_number", b, "--num", "1"])
    run_script("ingestion.get_albums", ["--page_number", p, "--batch_number", b, "--num", "1"])
    run_script("ingestion.get_songs", ["--page_number", p, "--batch_number", b, "--num", "2"])
    run_script("ingestion.get_isrc_and_pop", ["--page_number", p, "--batch_number", b, "--num", "3"])
    run_script("ingestion.group_songs", ["--page_number", p, "--batch_number", b])
    run_script("ingestion.get_streams", ["--page_number", p, "--batch_number", b, "--num", "1"])
    run_script("ingestion.create_parquet", ["--page_number", p, "--batch_number", b])
    run_script("ingestion.insert_db", ["--page_number", p, "--batch_number", b])

    logger.info(f"INGESTION FLOW COMPLETED for page {p} and batch {b}")

if __name__ == "__main__":
    ingestion_flow(page_number=1, batch_number=1)
