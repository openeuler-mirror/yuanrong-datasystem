"""Replace known HTML template tokens while reporting missing injections."""

import re


def replace_tokens(template, pattern, injections):
    def substitute(match):
        token = match.group(0)
        replacement = injections.get(token)
        if replacement is None:
            raise ValueError(f"missing template injection: {token}")
        return replacement

    return re.sub(pattern, substitute, template)
