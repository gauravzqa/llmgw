"""Load real provider credentials from a file that lives outside this repo.

--------------------------------------------------------------------------
Why the keys are not in this repository, and never will be
--------------------------------------------------------------------------

The tempting move is `cp ~/…/.env .env` and a `.gitignore` line. It is wrong
for a reason that has nothing to do with git: a secret's blast radius is the
set of processes that can read it, and copying multiplies that set forever
after. The copy does not expire when the original is rotated, it does not
move when the original moves, and the day someone runs `tar czf backup.tgz`
over this directory it leaves with everything else. A `.gitignore` entry
protects against exactly one of the many ways a file escapes.

So the credential file has ONE location, owned by whoever owns the
keys, and this module reads it at runtime by path. `LLMGW_ENV_FILE` overrides
the path; nothing overrides the rule.

--------------------------------------------------------------------------
Why this is 40 lines of stdlib and not `python-dotenv`
--------------------------------------------------------------------------

The file being parsed contains real provider credentials. Adding a dependency
to read it means a third-party package, its
maintainer, its release pipeline, and whatever it in turn imports all sit in
the trust path of that file. The parsing problem is `line.split("=", 1)` plus
three edge cases. That trade is not close.

--------------------------------------------------------------------------
Precedence: the environment always wins
--------------------------------------------------------------------------

`load_env()` never overwrites a variable that is already set. This is the
same rule `os.environ.setdefault` implements, and it exists so that
`ANTHROPIC_API_KEY=sk-broken pytest tests/live` actually tests a broken key
rather than silently testing the good one from the file. A loader that
clobbers the environment is a loader you cannot use to reproduce a failure.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ENV_FILE = os.path.expanduser("~/.config/llmgw/env")
"""Default credential file, outside the repo. Read, never copied, never written."""

ENV_FILE_VAR = "LLMGW_ENV_FILE"

PROVIDER_KEY_VARS: dict[str, str] = {
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
}
"""Provider id -> the env var its credential lives in.

`openai` has no entry in `catalog.PROVIDERS` today; it is
here because the key exists and `probe` can list its models, which is free
and is how we find out what the catalog *should* say.
"""


class MissingEnvFile(RuntimeError):
    """The credential file is not where we were told it would be.

    A distinct class because the recovery is distinct and specific: point
    `LLMGW_ENV_FILE` at the right path. Every other failure in this package
    is a network or a provider problem.
    """


@dataclass(frozen=True, slots=True)
class KeyStatus:
    """Presence and length of one credential. Deliberately never the value.

    `length` is here because "the key is set" and "the key is set to the
    empty string" are different states that a boolean cannot distinguish, and
    because a length that suddenly reads 3 tells you someone exported a
    placeholder. It is the most information you can print about a secret
    without printing any of it.
    """

    name: str
    present: bool
    length: int

    def __str__(self) -> str:
        return f"{self.name} {'set' if self.present else 'MISSING'} (len={self.length})"


def parse_env_text(text: str) -> dict[str, str]:
    """Parse `KEY=value` lines. Handles `export`, quotes, comments, blanks.

    Not a shell. `$FOO` is not expanded and `a=b c=d` is one variable whose
    value contains a space -- both because expanding would mean deciding what
    a partially-loaded environment means mid-parse, and because every real
    line in the file this reads is a flat opaque token.

    An inline `#` is NOT treated as a comment. A comment marker inside an
    unquoted value is indistinguishable from a `#` that is part of a
    generated secret, and truncating a credential at a character it legally
    contains produces a 401 whose cause is three layers from where it shows.
    Only a `#` in column one (after whitespace) starts a comment.
    """
    out: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        name, sep, value = line.partition("=")
        if not sep:
            continue
        name = name.strip()
        if not name or not (name[0].isalpha() or name[0] == "_"):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        out[name] = value
    return out


def env_file_path(path: str | os.PathLike[str] | None = None) -> Path:
    """Where the credentials are. Argument, then `LLMGW_ENV_FILE`, then default."""
    if path is not None:
        return Path(path)
    return Path(os.environ.get(ENV_FILE_VAR) or DEFAULT_ENV_FILE)


def load_env(
    path: str | os.PathLike[str] | None = None, *, override: bool = False
) -> list[str]:
    """Inject the file's variables into `os.environ`. Returns the names set.

    Names only -- the return value is designed to be printable. A function
    that hands back a `dict[str, str]` of secrets is one whose result ends up
    in a log line the first time someone debugs it.

    Raises `MissingEnvFile` naming the exact path when the file is absent. It
    does not search, guess, or fall back to a sibling: a credential loader
    that silently finds *a* `.env` is one that will eventually authenticate as
    the wrong account, and the error message would then be a lie.
    """
    file = env_file_path(path)
    if not file.is_file():
        raise MissingEnvFile(
            f"credential file not found: {file}\n"
            f"Set ${ENV_FILE_VAR} to the file that holds the provider keys. "
            f"This harness does not search for one, and no key is ever copied "
            f"into this repository."
        )
    injected: list[str] = []
    for name, value in parse_env_text(file.read_text(encoding="utf-8")).items():
        if override or name not in os.environ:
            os.environ[name] = value
            injected.append(name)
    return injected


def key_status(names: list[str] | None = None) -> list[KeyStatus]:
    """Presence report for the provider credentials. Values never leave."""
    wanted = names if names is not None else list(PROVIDER_KEY_VARS.values())
    out = []
    for name in wanted:
        value = os.environ.get(name) or ""
        out.append(KeyStatus(name=name, present=bool(value), length=len(value)))
    return out


def require(*names: str) -> None:
    """Fail loudly before spending a request on a credential that is not there."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        raise RuntimeError(
            f"missing credentials: {', '.join(missing)} -- "
            f"load them from {env_file_path()} or export them"
        )


def ensure_loaded(path: str | os.PathLike[str] | None = None) -> list[KeyStatus]:
    """Load the file if it is there and report which keys are now present.

    The one call every entry point in this package makes first.
    """
    load_env(path)
    return key_status()


if __name__ == "__main__":  # pragma: no cover - a presence check, by hand
    print(f"env file: {env_file_path()}")
    for status in ensure_loaded():
        print(f"  {status}")
