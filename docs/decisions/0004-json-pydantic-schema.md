# ADR-0004: JSON events validated by Pydantic

Status: accepted

## Context
Events need a defined schema, validation with clear failure reasons, and a path for
schema evolution. The pipeline runs on a single machine.

## Decision
Encode events as JSON with a schema_version field (currently 1), validated by
Pydantic v2 and parsed with orjson.

## Rejected alternative
Avro + schema registry. It adds an extra service on a single machine, and events become
unreadable in rpk.

## Consequences
- Schema evolution is enforced in code, keyed on schema_version.
- Avro + schema registry is the documented upgrade path at 10x scale.
