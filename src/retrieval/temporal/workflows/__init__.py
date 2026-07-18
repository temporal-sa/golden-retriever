"""Retrieval Workflow Types.

Worker registration imports concrete modules explicitly so importing one workflow never
eagerly imports the entire topology (important for replay isolation and pure unit tests).
"""
