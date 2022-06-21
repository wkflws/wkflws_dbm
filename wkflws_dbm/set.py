import json
import os
from typing import Any

from . import cache


async def set(message: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
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

    try:
        value: str = message["value"]
    except KeyError:
        raise ValueError("'value' missing from Parameters") from None

    expiry_secs: int = int(message.get("expiry_secs", 300))

    await cache.set(db_path, key, value, expiry_secs=expiry_secs)

    return value  # type:ignore


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

    output = asyncio.run(set(message, context))

    # Non-zero exit codes indicate to the executor there was an unrecoverable error and
    # workflow execution should terminate.
    if output is None:
        sys.exit(1)

    # The output of your function is input for a potential next state. It must be in
    # JSON format and be the only thing output on stdout. This value is picked up by the
    # executor and processed.
    print(json.dumps(output))
