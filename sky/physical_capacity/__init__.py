"""Read-only PostgreSQL physical-capacity evidence projection.

Revision 001 persists only bounded scan summaries.  It exposes no capacity
inventory reader, workload mutation path, or provider observation loop.
"""
