#!/usr/bin/env python3
import csv
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def require(condition, message):
    if not condition:
        raise ValueError(message)


def table(name):
    path = ROOT / name
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    require(rows, f"{name}: no rows")
    require(all(None not in row and all(row.values()) for row in rows),
            f"{name}: empty or malformed cell")
    ids = [row["id"] for row in rows]
    require(len(ids) == len(set(ids)), f"{name}: duplicate ID")
    return rows


def check_markdown():
    documents = sorted(ROOT.rglob("*.md"))
    for path in documents:
        text = path.read_text()
        require(text.isascii(), f"{path.name}: non-ASCII text")
        require(len(re.findall(r"^# ", text, re.M)) == 1,
                f"{path.name}: expected one top-level heading")
        require(len(re.findall(r"^```", text, re.M)) % 2 == 0,
                f"{path.name}: unmatched code fence")
        require(all(line == line.rstrip() for line in text.splitlines()),
                f"{path.name}: trailing whitespace")
        for target in re.findall(r"\[[^\]]+\]\(([^)]+)\)", text):
            if "://" in target or target.startswith("#"):
                continue
            resolved = path.parent / target.split("#", 1)[0]
            require(resolved.is_file(), f"{path.name}: missing link {target}")
    return len(documents)


def check_matrix():
    requirements = table("requirements.tsv")
    cases = table("verification-matrix.tsv")
    requirement_ids = {row["id"] for row in requirements}
    case_by_id = {row["id"]: row for row in cases}
    covered = set()
    for requirement in requirements:
        require((ROOT / requirement["spec"]).is_file(),
                f'{requirement["id"]}: missing specification')
        for case_id in requirement["mandatory_test_ids"].split(","):
            require(case_id in case_by_id, f"missing matrix case {case_id}")
            require(case_by_id[case_id]["requirement"] == requirement["id"],
                    f"{case_id}: requirement mismatch")
            covered.add(case_id)
    require(covered == set(case_by_id), "matrix has unreferenced cases")
    require(all(row["requirement"] in requirement_ids for row in cases),
            "matrix refers to an unknown requirement")
    return len(requirements), len(cases)


def check_prompt_coverage():
    prompt = (ROOT / "IMPLEMENTATION_PROMPT.md").read_text()
    index = (ROOT / "README.md").read_text()
    for path in ROOT.glob("[0-9][0-9]-*.md"):
        require(path.name in prompt, f"prompt omits normative document {path.name}")
        require(f"]({path.name})" in index, f"index omits normative document {path.name}")


def check_catalog():
    catalog = json.loads((ROOT / "examples/activity-catalog.json").read_text())
    require(catalog["format"] == "graphrun.catalog/v1", "catalog version")
    schemas = catalog["schemas"]
    keys = set()
    for activity in catalog["activities"]:
        key = (activity["name"], activity["version"])
        require(key not in keys, f"duplicate activity {key}")
        keys.add(key)
        require(activity["input_schema"] in schemas, f"{key}: missing input schema")
        require(activity["output_schema"] in schemas, f"{key}: missing output schema")
        require(activity["execution"] in {"async", "blocking"}, f"{key}: execution")
        require(activity["effects"] in {"pure", "external"}, f"{key}: effects")
        require(activity["recovery"] in {"RetrySafe", "Manual"}, f"{key}: recovery")
    reconciler_keys = set()
    for reconciler in catalog["reconcilers"]:
        key = (reconciler["name"], reconciler["version"])
        require(key not in reconciler_keys, f"duplicate reconciler {key}")
        reconciler_keys.add(key)
        target = reconciler["forward_activity"]
        require((target["name"], target["version"]) in keys,
                f"{key}: missing forward activity")
    for activity in catalog["activities"]:
        if "reconciler" in activity:
            ref = activity["reconciler"]
            require((ref["name"], ref["version"]) in reconciler_keys,
                    f'{activity["name"]}: missing reconciler')
    return len(keys)


def check_primitive_examples():
    required = {
        "activity", "choose", "delay", "wait_until", "wait_signal", "complete",
        "fail", "while", "do_while", "repeat", "foreach", "parallel", "saga",
    }
    paths = sorted((ROOT / "examples").glob("*.yaml"))
    found = set()
    for path in paths:
        text = path.read_text()
        require("\t" not in text, f"{path.name}: YAML indentation contains tabs")
        found.update(re.findall(r"^\s*kind:\s*([a-z_]+)\s*$", text, re.M))
    require(required <= found, f"missing primitive examples: {sorted(required - found)}")
    return len(paths), len(required)


if __name__ == "__main__":
    documents = check_markdown()
    requirements, cases = check_matrix()
    check_prompt_coverage()
    activities = check_catalog()
    examples, primitives = check_primitive_examples()
    print(f"{documents} documents, {requirements} requirements, {cases} matrix cases, "
          f"{activities} activity contracts, {examples} YAML examples, "
          f"{primitives} primitive kinds: specification structure is consistent.")
    print("This does not execute the engine or replace its required runtime verification.")
