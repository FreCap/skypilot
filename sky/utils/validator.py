"""This module contains a custom validator for the JSON Schema specification.

The main motivation behind extending the existing JSON Schema validator is to
allow for case-insensitive enum matching since this is currently not supported
by the JSON Schema specification.
"""
import jsonschema


def case_insensitive_enum(validator, enums, instance, schema):
    del validator, schema  # Unused.
    # Leave non-string rejection to the schema's type validator.  Custom
    # validators are still invoked for sibling keywords when a type does not
    # match, including while evaluating nullable anyOf branches.
    if not isinstance(instance, str):
        return
    if instance.lower() not in [enum.lower() for enum in enums]:
        yield jsonschema.ValidationError(
            f'{instance!r} is not one of {enums!r}')


def case_sensitive_enum(validator, enums, instance, schema):
    del validator, schema  # Unused.
    if instance not in enums:
        yield jsonschema.ValidationError(
            f'{instance!r} is not one of {enums!r}')


# Move this to a function to delay initialization
def get_schema_validator():
    """Get the schema validator class, initializing it only when needed."""
    return jsonschema.validators.extend(
        jsonschema.Draft7Validator,
        validators={
            'case_insensitive_enum': case_insensitive_enum,
            'case_sensitive_enum': case_sensitive_enum
        })
