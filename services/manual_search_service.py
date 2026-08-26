import os
import re


BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

MANUAL_PATH = os.path.join(
    BASE_DIR,
    "knowledge_base",
    "window_ticket_manual.md"
)


def load_manual():
    if not os.path.exists(MANUAL_PATH):
        raise FileNotFoundError(
            f"Manual file not found: {MANUAL_PATH}"
        )

    with open(
        MANUAL_PATH,
        "r",
        encoding="utf-8"
    ) as file:
        return file.read()


def split_manual_into_sections(manual_text):
    parts = re.split(
        r"(?=^# )",
        manual_text,
        flags=re.MULTILINE
    )

    sections = []

    for part in parts:
        part = part.strip()

        if not part:
            continue

        lines = part.splitlines()

        title = lines[0].replace(
            "#",
            ""
        ).strip()

        sections.append(
            {
                "title": title,
                "content": part
            }
        )

    return sections


def tokenize(text):
    words = re.findall(
        r"[a-zA-Z0-9]+",
        text.lower()
    )

    return set(words)


def score_section(query, section):
    query_lower = query.lower()
    content_lower = section["content"].lower()
    title_lower = section["title"].lower()

    query_words = tokenize(query)

    score = 0

    if query_lower in content_lower:
        score += 20

    if query_lower in title_lower:
        score += 30

    for word in query_words:

        if word in title_lower:
            score += 5

        if word in content_lower:
            score += 1

    keywords_match = re.search(
        r"Keywords:\s*(.+)",
        section["content"],
        flags=re.IGNORECASE
    )

    if keywords_match:

        keywords_text = keywords_match.group(1).lower()

        for word in query_words:
            if word in keywords_text:
                score += 4

    return score


def search_manual_service(
    query: str,
    limit: int = 3
):
    query = str(query or "").strip()

    if not query:
        return {
            "found": False,
            "message": "A search query is required.",
            "results": []
        }

    manual_text = load_manual()

    sections = split_manual_into_sections(
        manual_text
    )

    scored_sections = []

    for section in sections:

        score = score_section(
            query,
            section
        )

        if score > 0:
            scored_sections.append(
                (
                    score,
                    section
                )
            )

    scored_sections.sort(
        key=lambda item: item[0],
        reverse=True
    )

    results = []

    for score, section in scored_sections[:limit]:

        results.append(
            {
                "title": section["title"],
                "score": score,
                "content": section["content"]
            }
        )

    if not results:
        return {
            "found": False,
            "message": (
                "No relevant information was found "
                "in the Window Ticket user manual."
            ),
            "results": []
        }

    return {
        "found": True,
        "query": query,
        "results": results
    }