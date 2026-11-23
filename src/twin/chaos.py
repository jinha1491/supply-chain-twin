import argparse
import random
import subprocess
import uuid
from pathlib import Path


def latest_batch(folder: Path) -> int | None:
    if not folder.exists():
        return None
    ids = [int(p.name) for p in folder.iterdir() if p.name.isdigit()]
    return max(ids) if ids else None


def checkpoint_state(checkpoint: Path) -> str:
    planned = latest_batch(checkpoint / "offsets")
    committed = latest_batch(checkpoint / "commits")
    state = f"planned batch {planned}, committed batch {committed}"
    if planned is not None and planned != committed:
        state += " (batch in flight)"
    return state


def compose_run(name: str, job: list[str], log) -> subprocess.Popen:
    command = ["docker", "compose", "run", "--rm", "--name", name, "spark", *job]
    return subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kill a Spark job at random times, then finish it."
    )
    parser.add_argument("--kills", type=int, default=3)
    parser.add_argument("--min-wait", type=float, default=25)
    parser.add_argument("--max-wait", type=float, default=60)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--checkpoint", required=True, help="host path, like checkpoints/ingest_raw"
    )
    parser.add_argument("--log", default="chaos.log")
    parser.add_argument("job", nargs=argparse.REMAINDER, help="spark job and its arguments")
    args = parser.parse_args()

    job = args.job[1:] if args.job[:1] == ["--"] else args.job
    checkpoint = Path(args.checkpoint)
    rng = random.Random(args.seed)
    finished_early = False

    with open(args.log, "a") as log:
        for attempt in range(1, args.kills + 1):
            name = f"chaos-{uuid.uuid4().hex[:8]}"
            proc = compose_run(name, job, log)
            wait = rng.uniform(args.min_wait, args.max_wait)
            try:
                code = proc.wait(timeout=wait)
                print(f"attempt {attempt}: finished on its own (exit {code}) before the kill")
                finished_early = True
                break
            except subprocess.TimeoutExpired:
                killed = (
                    subprocess.run(["docker", "kill", name], capture_output=True).returncode == 0
                )
                proc.wait()
                result = "killed" if killed else "kill failed"
                print(
                    f"attempt {attempt}: {result} after {wait:.0f}s, {checkpoint_state(checkpoint)}"
                )

        if not finished_early:
            code = compose_run(f"chaos-final-{uuid.uuid4().hex[:8]}", job, log).wait()
            print(f"final run: exit {code}, {checkpoint_state(checkpoint)}")
            if code:
                raise SystemExit(code)


if __name__ == "__main__":
    main()
