import json
import os
from typing import Any

from . import cache


async def get(message: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Write data to a local store for later retrieval."""
    try:
        filename: str = message["filename"]
    except KeyError:
        raise ValueError("'filename' missing from Parameters") from None

    if "/" in filename or "\\" in filename or "." in filename:
        raise ValueError("'file must not contain '/', '\\', '.'.")

    db_path = os.path.join("/tmp", f"{filename}.dbm")

    try:
        key: str = message["key"]
    except KeyError:
        raise ValueError("'key' missing from Parameters") from None

    default = message.get("default", None)

    value = await cache.get(db_path, key, default)

    return value


if __name__ == "__main__":
    import asyncio
    import sys

    # message is the input to your function. This is the output from the previous
    # function plus any transformations the user defined in their workflow. Parameters
    # should be documented in the parameters.json file so they can be used in the UI.
    try:
        message = json.loads(sys.argv[1])
    except IndexError:
        raise ValueError("missing required `message` argument") from None

    # this contains some contextual information about the workflow and the current
    # state. required secrets should be defined in the README so users can write their
    # lookup class with this node's unique requirements in mind.
    try:
        context = json.loads(sys.argv[2])
    except IndexError:
        raise ValueError("missing `context` argument") from None

    output = asyncio.run(get(message, context))

    # Non-zero exit codes indicate to the executor there was an unrecoverable error and
    # workflow execution should terminate.
    if output is None:
        sys.exit(1)

    # The output of your function is input for a potential next state. It must be in
    # JSON format and be the only thing output on stdout. This value is picked up by the
    # executor and processed.
    print(json.dumps(output))
