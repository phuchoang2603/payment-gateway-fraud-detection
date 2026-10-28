import re


def immutable_image(value):
    if not re.fullmatch(r"[^\s@]+(?:@sha256:[a-f0-9]{64}|:(?:pr-[0-9]+|main)-[a-f0-9]{40})", value):
        raise ValueError("image reference must identify a digest or immutable release tag")
    return value
