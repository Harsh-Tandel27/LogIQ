"""Send one Slack message per incident through an Incoming Webhook."""

import logging

import requests

from logiq import config

log = logging.getLogger("logiq.slack")

SEVERITY_EMOJI = {"High": ":red_circle:", "Medium": ":large_orange_circle:", "Low": ":white_circle:"}


def _post(payload: dict) -> bool:
    if not config.SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set, Slack message not sent:\n%s", payload["text"])
        return False
    try:
        response = requests.post(config.SLACK_WEBHOOK_URL, json=payload, timeout=5)
        response.raise_for_status()
        return True
    except requests.RequestException as exc:
        log.error("Slack webhook failed: %s", exc)
        return False


def notify_incident(incident: dict) -> bool:
    url = f"{config.FRONTEND_URL}/incident/{incident['_id']}"
    affected = ", ".join(s for s in incident["affected_services"] if s != incident["root_cause"]) or "none"
    fix = incident["recommendations"][0]["fix"] if incident["recommendations"] else "No matching runbook yet."
    text = (
        f"{SEVERITY_EMOJI.get(incident['severity'], '')} *{incident['severity']} incident {incident['_id']}*: "
        f"root cause is *{incident['root_cause']}*"
    )
    return _post({
        "text": text,
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
            {"type": "section", "fields": [
                {"type": "mrkdwn", "text": f"*Failure path*\n{' → '.join(incident['chain'])}"},
                {"type": "mrkdwn", "text": f"*Also affected*\n{affected}"},
                {"type": "mrkdwn", "text": f"*Error logs grouped*\n{incident['total_logs']:,} logs into {len(incident['alert_ids'])} alerts"},
                {"type": "mrkdwn", "text": f"*Suggested fix*\n{fix}"},
            ]},
            {"type": "actions", "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": "Open incident report"}, "url": url},
            ]},
        ],
    })


def notify_root_cause_changed(incident: dict, previous_root: str) -> bool:
    url = f"{config.FRONTEND_URL}/incident/{incident['_id']}"
    return _post({
        "text": f"Update on {incident['_id']}: root cause moved from {previous_root} to *{incident['root_cause']}* "
                f"(new errors arrived further down the dependency chain). <{url}|Open incident report>",
    })
