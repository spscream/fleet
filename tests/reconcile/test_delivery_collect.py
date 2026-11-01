"""Exercise receipt persistence through fresh collect processes, not helper calls."""
import json
import os
from pathlib import Path

import pytest
import yaml

from harness import run, run_queue as q, write
from test_notify_origin import SID, session, task


def memory():
    return Path(os.environ["FLEET_RECONCILE_DIR"]) / "origin_outcomes.json"


def read_memory():
    return json.loads(memory().read_text(encoding="utf-8"))


def collect():
    result = q("collect", "--no-reap")
    assert result.code == 0, result.out
    return result.out


def seed(stubs, slug="one"):
    memory().parent.mkdir(exist_ok=True)
    ref = task(slug, f"thurbox:{SID}")
    directory = Path(os.environ["FLEET_QUEUE_DIR"]) / ref
    doc = yaml.safe_load((directory / "task.yaml").read_text(encoding="utf-8"))
    doc.update(state="dispatched", publish={"method": "pr"})
    write(directory / "task.yaml", yaml.safe_dump(doc))
    number = int(ref.split("/")[1].split("-")[0])
    stubs.plain_pr(number, f"fix/{slug}")
    write(directory / "result.md",
          f"---\noutcome: shipped\nartifact: https://github.com/example/project/pull/{number}\n---\nSynthetic result.\n")
    return ref + ":shipped"


def inbox(stubs):
    return stubs.inbox(SID)


def wakes(stubs):
    return stubs.calls("thurbox-cli", "session send")


def claim():
    result = run(["thurbox-cli", "message", "inbox", "--for", SID, "--claim", "--json"])
    assert result.code == 0, result.out


def test_collect_zero_results_restart_busy_idle_claim(stubs):
    key = seed(stubs)
    session(stubs, "origin", "Recipient", SID, state="working")
    collect()
    entry = read_memory()["pending"][key]
    assert entry["message_id"] == inbox(stubs)[0]["id"]
    assert entry["mailbox_session"] == SID
    assert wakes(stubs) == []
    assert "0 result(s) read" in collect()
    assert len(inbox(stubs)) == 1
    session(stubs, "origin", "Recipient", SID)
    assert "0 result(s) read" in collect()
    assert len(wakes(stubs)) == 1
    assert "message inbox --claim" in wakes(stubs)[0]
    assert inbox(stubs)[0]["body"] not in wakes(stubs)[0]
    assert key in read_memory()["pending"], "typing alone is not an acknowledgement"
    claim()
    collect()
    assert key in read_memory()["sent"]
    assert not read_memory()["pending"]
    collect()
    assert len(wakes(stubs)) == 1


@pytest.mark.parametrize("legacy", [False, True])
def test_posted_claimed_is_not_retyped_even_after_restart(stubs, legacy):
    key = seed(stubs)
    session(stubs, "origin", "Recipient", SID, state="working")
    collect()
    if legacy:
        doc = read_memory()
        entry = doc["pending"][key]
        for field in ("message_id", "mailbox_session", "posting", "mailbox_body"):
            entry.pop(field, None)
        write(memory(), json.dumps(doc))
        row = inbox(stubs)[0]
        row["body"] = entry["line"]
        write(stubs.root / "inbox" / f"{SID}.jsonl", json.dumps(row) + "\n")
    claim()
    session(stubs, "origin", "Recipient", SID)
    collect()
    assert wakes(stubs) == []
    assert key in read_memory()["sent"]
    assert not read_memory()["pending"]


@pytest.mark.parametrize("answer", ["[]", "bad json", "{}", "failure"])
def test_unknown_inbox_keeps_pending_and_does_not_repost_or_type(stubs, answer):
    key = seed(stubs)
    session(stubs, "origin", "Recipient", SID, state="working")
    collect()
    before = read_memory()["pending"][key]
    session(stubs, "origin", "Recipient", SID)
    write(stubs.root / "inbox-answer", answer)
    collect()
    assert read_memory()["pending"][key] == before
    assert wakes(stubs) == []


def test_ambiguous_enqueue_recovers_identity_without_second_enqueue(stubs):
    key = seed(stubs)
    session(stubs, "origin", "Recipient", SID, state="working")
    # Enqueue succeeds durably but the CLI reports failure after that mutation.
    write(stubs.root / "enqueue-fail-after-write", "1")
    collect()
    assert read_memory()["pending"][key]["message_id"] == inbox(stubs)[0]["id"]
    collect()
    assert len(inbox(stubs)) == 1
    assert len(stubs.calls("thurbox-cli", "message send")) == 1
    claim()
    collect()
    assert key in read_memory()["sent"]


def test_unavailable_recipient_keeps_posted_record(stubs):
    key = seed(stubs)
    session(stubs, "origin", "Recipient", SID, state="working")
    collect()
    (stubs.root / "sessions" / "origin.json").unlink()
    collect()
    assert key in read_memory()["pending"]
    assert wakes(stubs) == []
    session(stubs, "origin", "Recipient", SID)
    claim()
    collect()
    assert key in read_memory()["sent"]


def test_multiple_results_are_delivered_in_order_across_collects(stubs):
    first = seed(stubs, "one")
    second = seed(stubs, "two")
    session(stubs, "origin", "Recipient", SID, state="working")
    collect()
    assert list(read_memory()["pending"]) == [first, second]
    assert len(inbox(stubs)) == 1
    session(stubs, "origin", "Recipient", SID)
    collect()
    assert len(wakes(stubs)) == 1
    claim()
    collect()
    assert read_memory()["sent"] == [first]
    assert len(inbox(stubs)) == 2
    assert "one shipped" in inbox(stubs)[0]["body"]
    assert "two shipped" in inbox(stubs)[1]["body"]
    claim()
    collect()
    assert read_memory()["sent"] == [first, second]
    collect()
    assert len(inbox(stubs)) == 2


@pytest.mark.parametrize("change", ["recipient", "body", "kind", "id", "read_at", "duplicate"])
def test_mismatching_or_ambiguous_records_never_acknowledge(stubs, change):
    key = seed(stubs)
    session(stubs, "origin", "Recipient", SID, state="working")
    collect()
    row = inbox(stubs)[0]
    if change == "duplicate":
        doc = read_memory()
        doc["pending"][key].pop("message_id")
        write(memory(), json.dumps(doc))
        answer = [row, {**row, "id": row["id"] + 1}]
    else:
        field, value = {
            "recipient": ("to_session_id", "another-recipient"),
            "body": ("body", "unrelated payload"), "kind": ("kind", "another-kind"),
            "id": ("id", row["id"] + 1), "read_at": ("read_at", "unknown"),
        }[change]
        answer = [{**row, field: value}]
    write(stubs.root / "inbox-answer", json.dumps(answer))
    session(stubs, "origin", "Recipient", SID)
    collect()
    assert key in read_memory()["pending"]
    assert wakes(stubs) == []
    assert len(stubs.calls("thurbox-cli", "message send")) == 1


def test_restart_after_saved_intent_recovers_without_enqueue(stubs):
    key = seed(stubs)
    session(stubs, "origin", "Recipient", SID, state="working")
    collect()
    doc = read_memory()
    entry = doc["pending"][key]
    entry.pop("message_id")
    entry.pop("posted")
    write(memory(), json.dumps(doc))
    claim()
    collect()
    assert key in read_memory()["sent"]
    assert len(stubs.calls("thurbox-cli", "message send")) == 1
    assert wakes(stubs) == []


def test_no_runtime_still_attempts_every_event(stubs):
    first = seed(stubs, "one")
    second = seed(stubs, "two")
    memory().parent.rmdir()
    session(stubs, "origin", "Recipient", SID, state="working")
    collect()
    assert not memory().exists()
    assert [first.split(":")[0] in inbox(stubs)[0]["body"],
            second.split(":")[0] in inbox(stubs)[1]["body"]] == [True, True]


def test_new_declaration_cannot_claim_an_old_identical_question(stubs, tmp_path):
    from test_notify_origin import asks, notify
    ref = task("question", f"thurbox:{SID}")
    session(stubs, "origin", "Recipient", SID, state="working")
    notify(tmp_path, asking=asks(ref))
    claim()
    notify(tmp_path, asking=asks(ref))
    assert json.loads((tmp_path / "origin_questions.json").read_text(encoding="utf-8"))["asked"][ref]["done"]
    write(stubs.root / "enqueue-fail-before-write", "1")
    notify(tmp_path, asking=asks(ref, at="2026-10-13T11:00:00+00:00"))
    entry = json.loads((tmp_path / "origin_questions.json").read_text(encoding="utf-8"))["asked"][ref]
    assert not entry.get("done")
    assert len(inbox(stubs)) == 1


def test_posted_question_survives_temporarily_missing_recipient(stubs, tmp_path):
    from test_notify_origin import asks, notify
    ref = task("question", f"thurbox:{SID}")
    session(stubs, "origin", "Recipient", SID, state="working")
    notify(tmp_path, asking=asks(ref))
    (stubs.root / "sessions" / "origin.json").unlink()
    notify(tmp_path, asking=asks(ref))
    entry = json.loads((tmp_path / "origin_questions.json").read_text(encoding="utf-8"))["asked"][ref]
    assert not entry.get("done")
    session(stubs, "origin", "Recipient", SID)
    notify(tmp_path, asking=asks(ref))
    assert len(wakes(stubs)) == 1
    claim()
    notify(tmp_path, asking=asks(ref))
    assert json.loads((tmp_path / "origin_questions.json").read_text(encoding="utf-8"))["asked"][ref]["done"]


@pytest.mark.parametrize("failure", ["memory", "start"])
def test_known_pre_send_failure_is_retried_through_collect(stubs, failure):
    from harness import PYTHON
    key = seed(stubs)
    session(stubs, "origin", "Recipient", SID, state="working")
    script = '''from fleet.cli import load, main
n = load("notify_origin.py")
original_write = n.fleetqueue.fleet_platform.write_record
original_start = n.delivery.subprocess.Popen
failed = False

def write(path, text):
    global failed
    if not failed and str(path).endswith(n.OUTCOMES_FILE):
        failed = True
        raise OSError("synthetic memory failure")
    return original_write(path, text)

def start(args, **kwargs):
    global failed
    if not failed and args[:3] == ["thurbox-cli", "message", "send"]:
        failed = True
        raise OSError("synthetic process start failure")
    return original_start(args, **kwargs)
'''
    script += ('n.fleetqueue.fleet_platform.write_record = write\n' if failure == "memory" else
               'n.delivery.subprocess.Popen = start\n')
    script += 'raise SystemExit(main(["queue", "collect", "--no-reap"]))\n'
    result = run([*PYTHON, "-c", script])
    assert result.code == 0, result.out
    assert not read_memory()["pending"][key].get("posting")
    assert inbox(stubs) == []
    collect()
    assert read_memory()["pending"][key]["message_id"] == inbox(stubs)[0]["id"]
    assert len(inbox(stubs)) == 1


def test_two_routes_to_same_session_share_delivery_order(stubs):
    first = seed(stubs, "one")
    second = seed(stubs, "two")
    directory = Path(os.environ["FLEET_QUEUE_DIR"]) / second.removesuffix(":shipped")
    doc = yaml.safe_load((directory / "task.yaml").read_text(encoding="utf-8"))
    doc["origin"] = "chat:synthetic"
    write(directory / "task.yaml", yaml.safe_dump(doc))
    write(Path(os.environ["FLEET_OWNERS_ROOT"]) / "orchestration" / "owners.conf", "chat: = Recipient\n")
    session(stubs, "origin", "Recipient", SID, state="working", backend_type="local:tmux")
    collect()
    assert list(read_memory()["pending"]) == [first, second]
    assert len(inbox(stubs)) == 1
    claim()
    collect()
    assert read_memory()["sent"] == [first]
    assert len(inbox(stubs)) == 2


def backlog(stubs, count):
    """Claimed history must still count toward the CLI's oldest-first limit."""
    rows = [{"id": i + 1, "to_session_id": SID, "kind": "reply",
             "body": f"older message {i}", "read_at": 1} for i in range(count)]
    write(stubs.root / "inbox" / f"{SID}.jsonl",
          "".join(json.dumps(row) + "\n" for row in rows))


def delivery_route(stubs, tmp_path, route):
    from test_notify_origin import asks, notify
    session(stubs, "origin", "Recipient", SID, state="working")
    if route == "outcome":
        key = seed(stubs)
        return collect, lambda: read_memory()["pending"].get(key), lambda: key in read_memory()["sent"]
    ref = task("question", f"thurbox:{SID}")

    def entry():
        return json.loads((tmp_path / "origin_questions.json").read_text(encoding="utf-8"))["asked"][ref]

    return lambda: notify(tmp_path, asking=asks(ref)), entry, lambda: entry().get("done", False)


@pytest.mark.parametrize("route", ["outcome", "question"])
@pytest.mark.parametrize("claimed", [False, True])
def test_receipt_beyond_default_page_through_notifier(stubs, tmp_path, route, claimed):
    backlog(stubs, 110)
    tick, entry, done = delivery_route(stubs, tmp_path, route)
    tick()
    assert entry()["message_id"] == 111
    assert len(inbox(stubs)) == 111
    if claimed:
        claim()
    session(stubs, "origin", "Recipient", SID)
    tick()
    assert done() is claimed
    if claimed:
        assert wakes(stubs) == []
    else:
        assert len(wakes(stubs)) == 1
        assert "message inbox --claim --limit 1 --json" in wakes(stubs)[0]
        assert inbox(stubs)[-1]["body"] not in wakes(stubs)[0]
        claim()
        tick()
        assert done()
    assert len(stubs.calls("thurbox-cli", "message send")) == 1


@pytest.mark.parametrize("route", ["outcome", "question"])
@pytest.mark.parametrize("answer", ["saturated", "failure", "bad json", "duplicate", "foreign"])
def test_incomplete_or_untrusted_snapshot_keeps_both_routes_pending(stubs, tmp_path, route, answer):
    backlog(stubs, 110)
    tick, entry, done = delivery_route(stubs, tmp_path, route)
    tick()
    before = dict(entry())
    row = {**inbox(stubs)[-1], "read_at": 1}
    if answer == "saturated":
        # The expected identity is present, but unseen rows could duplicate it.
        rows = [row] + inbox(stubs)[:1] * 4095
        answer = json.dumps(rows)
    elif answer == "duplicate":
        answer = json.dumps([row, row])
    elif answer == "foreign":
        answer = json.dumps([{**row, "to_session_id": "foreign-recipient"}])
    write(stubs.root / "inbox-answer", answer)
    session(stubs, "origin", "Recipient", SID)
    tick()
    assert not done()
    assert entry() == before
    assert wakes(stubs) == []
    assert len(stubs.calls("thurbox-cli", "message send")) == 1


@pytest.mark.parametrize("route", ["outcome", "question"])
def test_unknown_enqueue_beyond_default_page_recovers_without_repeat(stubs, tmp_path, route):
    backlog(stubs, 110)
    tick, entry, done = delivery_route(stubs, tmp_path, route)
    write(stubs.root / "enqueue-fail-after-write", "1")
    tick()
    assert entry()["message_id"] == 111
    claim()
    tick()
    assert done()
    assert wakes(stubs) == []
    assert len(stubs.calls("thurbox-cli", "message send")) == 1
