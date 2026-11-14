#!/usr/bin/env python3
"""Dependency-free structural validation of the handoff package.
It does not replace buf lint, real DeepFlow contract tests or Go integration tests.
"""
from pathlib import Path
import csv,json,sys
ROOT=Path(__file__).resolve().parents[1]
required=[
    'README.md','buf.yaml','buf.gen.yaml','go.mod','Makefile',
    'proto/alert/event/v1/event.proto','proto/alert/ingest/v1/ingest.proto',
    'contracts/external/deepflow/v7.2.3/bridge.schema.json',
    'planning/backlog.csv','planning/week_schedule.csv',
    'migrations/0001_initial.up.sql',
    'planning/DEVELOPMENT_LOG.md',
    'scripts/dev_up.sh','etc/config.yaml','scripts/breaking_drill.sh','deploy/test/pg-test.yaml','Dockerfile','deploy/dev/docker-compose.yaml','scripts/smoke_e2e.sh',
    'docs/09-测试与验收.md','docs/10-研发排期与里程碑.md',
    'docs/13-分阶段任务验收标准.md'
]
for item in required:
    assert (ROOT/item).is_file(),f'missing: {item}'
for file in ROOT.rglob('*.json'):
    json.loads(file.read_text(encoding='utf-8'))
for file in ROOT.rglob('*.proto'):
    content=file.read_text(encoding='utf-8')
    assert 'syntax =' in content and 'package ' in content, file
with (ROOT/'planning/backlog.csv').open(newline='',encoding='utf-8-sig') as f:
    tasks=list(csv.DictReader(f))
assert len(tasks)>=40, f'expected >=40 tasks; got {len(tasks)}'
with (ROOT/'planning/week_schedule.csv').open(newline='',encoding='utf-8-sig') as f:
    weeks=list(csv.DictReader(f))
assert len(weeks)==20 and {r['week'] for r in weeks}=={f'W{i:02d}' for i in range(1,21)}
print(f'OK: {len(required)} required files, {len(tasks)} backlog items, {len(weeks)} schedule weeks; JSON parsed.')
