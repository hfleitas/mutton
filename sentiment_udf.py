import fabric.functions as fn
import logging
import re

udf = fn.UserDataFunctions()


def parse_transcript(content: str) -> list:
    """Helper: parse dialog lines from transcript text."""
    pattern = re.compile(r'^\[(\d{2}:\d{2})\]\s+(.+?)\s+\((.+?)\):\s+(.+)$', re.MULTILINE)
    results = []
    lineNum = 0
    for m in pattern.finditer(content):
        lineNum += 1
        results.append({
            "lineNumber": lineNum,
            "timestamp": m.group(1),
            "speaker": m.group(2).strip(),
            "role": m.group(3).strip(),
            "text": m.group(4).strip()
        })
    return results


_POSITIVE_WORDS = {
    "thank", "thanks", "good", "great", "glad", "appreciate", "helpful",
    "perfect", "excellent", "wonderful", "happy", "pleased", "fortunate",
    "welcome", "sure", "absolutely", "okay", "yes", "right", "fine",
    "assist", "help", "resolve", "covered", "safe", "relief"
}
_NEGATIVE_WORDS = {
    "accident", "damage", "hit", "crash", "rear-ended", "frustrated",
    "sorry", "unfortunately", "problem", "issue", "worry", "concern",
    "injured", "hurt", "pain", "terrible", "awful", "bad", "worse",
    "fault", "blame", "delay", "difficult", "stressed", "upset",
    "wreck", "collision", "broken", "shaken"
}


def score_sentiment(text: str) -> dict:
    """Helper: compute sentiment using keyword matching (no external deps)."""
    words = set(re.findall(r'[a-z\-]+', text.lower()))
    pos_count = len(words & _POSITIVE_WORDS)
    neg_count = len(words & _NEGATIVE_WORDS)
    total = pos_count + neg_count

    if total == 0:
        polarity = 0.0
    else:
        polarity = (pos_count - neg_count) / total  # -1.0 to 1.0

    if polarity > 0.1:
        sentiment = "positive"
    elif polarity < -0.1:
        sentiment = "negative"
    else:
        sentiment = "neutral"

    return {
        "sentiment": sentiment,
        "polarity": round(polarity, 4),
        "confidencePositive": round(max(polarity, 0), 4),
        "confidenceNeutral": round(1 - abs(polarity), 4),
        "confidenceNegative": round(max(-polarity, 0), 4)
    }


@udf.connection(argName="lakehouse", alias="LH1")
@udf.function()
async def analyze_transcript_sentiment(lakehouse: fn.FabricLakehouseClient, fileName: str) -> list:
    """
    Reads a transcript file from Lakehouse Files/, parses each dialog line,
    runs sentiment analysis, and returns scored results.

    Args:
        lakehouse: Connection to LH1 Lakehouse.
        fileName: Name of the transcript file in Files/ (e.g. claims_call_transcript.txt).

    Returns:
        List of dicts with speaker, text, sentiment, and confidence scores.
    """
    logging.info(f"Reading transcript: {fileName}")

    # Connect to Lakehouse Files
    connection = lakehouse.connectToFilesAsync()
    fileClient = connection.get_file_client(fileName)
    download = await fileClient.download_file()
    rawBytes = await download.readall()
    content = rawBytes.decode("utf-8")
    fileClient.close()
    connection.close()

    # Parse dialog lines
    lines = parse_transcript(content)
    logging.info(f"Parsed {len(lines)} dialog lines")

    # Score sentiment for each line
    results = []
    for line in lines:
        scores = score_sentiment(line["text"])
        results.append({
            "lineNumber": line["lineNumber"],
            "timestamp": line["timestamp"],
            "speaker": line["speaker"],
            "role": line["role"],
            "text": line["text"],
            "sentiment": scores["sentiment"],
            "polarity": scores["polarity"],
            "confidencePositive": scores["confidencePositive"],
            "confidenceNeutral": scores["confidenceNeutral"],
            "confidenceNegative": scores["confidenceNegative"]
        })

    logging.info(f"Sentiment analysis complete. {len(results)} lines scored.")
    return results
