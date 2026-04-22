import json
import os
from random import shuffle
import sqlite3

from flask import Flask, abort, redirect, render_template, request, url_for

app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), "magic_brackets.db")


@app.context_processor
def inject_site_name():
    return {"site_name": "MTG Bracket Forge"}


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_database():
    with get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tournaments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                player_count INTEGER NOT NULL,
                player_names_json TEXT NOT NULL,
                pairing_order_json TEXT NOT NULL,
                round_scores_json TEXT NOT NULL DEFAULT '{}',
                finals_scores_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'in progress',
                champion_name TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )


initialize_database()


def normalize_round_scores(raw_scores):
    normalized = {}
    for round_number, pods in (raw_scores or {}).items():
        round_key = int(round_number)
        normalized[round_key] = {}
        for pod_number, player_scores in (pods or {}).items():
            pod_key = int(pod_number)
            normalized[round_key][pod_key] = {}
            for slot_number, score_entry in (player_scores or {}).items():
                normalized[round_key][pod_key][int(slot_number)] = score_entry
    return normalized


def normalize_finals_scores(raw_scores):
    normalized = {}
    for slot_number, score_entry in (raw_scores or {}).items():
        normalized[int(slot_number)] = score_entry
    return normalized


def serialize_tournament(tournament):
    return {
        "name": tournament["name"],
        "player_count": tournament["player_count"],
        "player_names_json": json.dumps(tournament["player_names"]),
        "pairing_order_json": json.dumps(tournament["pairing_order"]),
        "round_scores_json": json.dumps(tournament["round_scores"]),
        "finals_scores_json": json.dumps(tournament["finals_scores"]),
        "status": tournament["status"],
        "champion_name": tournament["champion_name"],
    }


def deserialize_tournament(row):
    if row is None:
        return None

    return {
        "id": row["id"],
        "name": row["name"],
        "player_count": row["player_count"],
        "player_names": json.loads(row["player_names_json"]),
        "pairing_order": json.loads(row["pairing_order_json"]),
        "round_scores": normalize_round_scores(json.loads(row["round_scores_json"])),
        "finals_scores": normalize_finals_scores(json.loads(row["finals_scores_json"])),
        "status": row["status"],
        "champion_name": row["champion_name"],
    }


def save_tournament(tournament):
    payload = serialize_tournament(tournament)
    with get_db_connection() as conn:
        conn.execute(
            """
            UPDATE tournaments
            SET name = ?,
                player_count = ?,
                player_names_json = ?,
                pairing_order_json = ?,
                round_scores_json = ?,
                finals_scores_json = ?,
                status = ?,
                champion_name = ?,
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (
                payload["name"],
                payload["player_count"],
                payload["player_names_json"],
                payload["pairing_order_json"],
                payload["round_scores_json"],
                payload["finals_scores_json"],
                payload["status"],
                payload["champion_name"],
                tournament["id"],
            ),
        )


def load_tournament(tournament_id):
    with get_db_connection() as conn:
        row = conn.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
    tournament = deserialize_tournament(row)
    if not tournament:
        abort(404)
    return tournament


def list_active_tournaments():
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM tournaments WHERE status != 'complete' ORDER BY id DESC"
        ).fetchall()
    tournaments = []
    for row in rows:
        tournament = deserialize_tournament(row)
        tournaments.append(enrich_tournament(tournament, persist=False))
    return tournaments


def list_all_tournaments():
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM tournaments ORDER BY id DESC"
        ).fetchall()
    tournaments = []
    for row in rows:
        tournament = deserialize_tournament(row)
        tournaments.append(enrich_tournament(tournament, persist=False))
    return tournaments


def normalize_player_names(raw_names, player_count):
    names = [name.strip() for name in raw_names.splitlines() if name.strip()]
    names = names[:player_count]
    while len(names) < player_count:
        names.append(f"Player {len(names) + 1}")
    return names


def create_tournament(name, player_count, player_names):
    pairing_order = player_names[:]
    shuffle(pairing_order)

    with get_db_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO tournaments (
                name,
                player_count,
                player_names_json,
                pairing_order_json,
                round_scores_json,
                finals_scores_json,
                status,
                champion_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name or "Untitled Tournament",
                player_count,
                json.dumps(player_names),
                json.dumps(pairing_order),
                json.dumps({}),
                json.dumps({}),
                "in progress",
                None,
            ),
        )
        tournament_id = cursor.lastrowid

    return tournament_id


def initialize_stats(player_names):
    return {
        name: {
            "name": name,
            "wins": 0,
            "points": 0,
            "rounds_played": 0,
            "opponents": [],
            "opponent_match_win_pct": 0.0,
        }
        for name in player_names
    }


def update_opponent_strength(stats):
    match_win_pct = {}
    for name, entry in stats.items():
        rounds_played = max(1, entry["rounds_played"])
        match_win_pct[name] = entry["wins"] / rounds_played

    for name, entry in stats.items():
        if not entry["opponents"]:
            entry["opponent_match_win_pct"] = 0.0
            continue
        total = sum(match_win_pct[opponent] for opponent in entry["opponents"])
        entry["opponent_match_win_pct"] = total / len(entry["opponents"])


def rank_players(stats, player_names):
    update_opponent_strength(stats)
    return sorted(
        player_names,
        key=lambda name: (
            -stats[name]["wins"],
            -stats[name]["opponent_match_win_pct"],
            -stats[name]["points"],
            name.lower(),
        ),
    )


def select_next_player(unassigned_players, current_pod, stats):
    best_index = 0
    best_repeat_score = None

    current_names = [player["name"] for player in current_pod]
    for index, candidate in enumerate(unassigned_players):
        repeat_score = sum(stats[candidate]["opponents"].count(name) for name in current_names)
        if best_repeat_score is None or repeat_score < best_repeat_score:
            best_repeat_score = repeat_score
            best_index = index

    return unassigned_players.pop(best_index)


def build_commander_pods(ordered_players, stats, is_finals_round=False):
    unassigned = ordered_players[:]
    pods = []
    pod_number = 1

    while unassigned:
        pod = [{"slot_number": 1, "name": unassigned.pop(0)}]

        while len(pod) < 4 and unassigned:
            selected_name = select_next_player(unassigned, pod, stats)
            pod.append({"slot_number": len(pod) + 1, "name": selected_name})

        while len(pod) < 4:
            pod.append({"slot_number": len(pod) + 1, "name": "BYE"})

        pod_data = {"pod_number": pod_number, "players": pod}
        if is_finals_round and pod_number == 1:
            pod_data["is_finals"] = True
        pods.append(pod_data)
        pod_number += 1

    return pods


def build_round_score_map(form_data, round_number, pods):
    round_scores = {}

    for pod in pods:
        pod_scores = {}
        for player in pod["players"]:
            if player["name"] == "BYE":
                continue

            field_name = f"score_{round_number}_{pod['pod_number']}_{player['slot_number']}"
            raw_score, parsed_score = parse_score(form_data, field_name)
            pod_scores[player["slot_number"]] = {
                "raw_score": raw_score,
                "score": parsed_score,
            }

        round_scores[pod["pod_number"]] = pod_scores

    return round_scores


def parse_score(form_data, field_name):
    raw_score = form_data.get(field_name, "").strip()
    if raw_score == "":
        return raw_score, None

    try:
        return raw_score, int(raw_score)
    except ValueError:
        return raw_score, None


def evaluate_round(round_number, pods, score_map, stats, apply_results):
    scored_pods = []
    round_complete = True

    for pod in pods:
        scored_players = []
        participants = []
        for player in pod["players"]:
            score_entry = score_map.get(pod["pod_number"], {}).get(player["slot_number"], {})
            raw_score = score_entry.get("raw_score", "")
            parsed_score = score_entry.get("score")
            field_name = f"score_{round_number}_{pod['pod_number']}_{player['slot_number']}"

            scored_player = {
                "slot_number": player["slot_number"],
                "name": player["name"],
                "raw_score": raw_score,
                "score": parsed_score,
                "field_name": field_name,
            }
            scored_players.append(scored_player)

            if player["name"] != "BYE":
                participants.append(scored_player)

        pod_complete = all(player["score"] is not None for player in participants) and bool(participants)
        round_complete = round_complete and pod_complete

        result_text = "Pending scores"
        tie = False
        winners = []

        if apply_results and pod_complete:
            participant_names = [player["name"] for player in participants]
            for name in participant_names:
                stats[name]["rounds_played"] += 1
                for opponent_name in participant_names:
                    if opponent_name != name:
                        stats[name]["opponents"].append(opponent_name)

            # Sort participants by score (descending)
            sorted_participants = sorted(participants, key=lambda p: p["score"], reverse=True)
            
            # Allocate points and wins based on placement
            placement_points = [3, 2, 1, 0]  # 1st, 2nd, 3rd, 4th
            result_lines = []
            
            for placement_idx, player in enumerate(sorted_participants):
                if placement_idx < len(placement_points):
                    player["points_earned"] = placement_points[placement_idx]
                    stats[player["name"]]["points"] += placement_points[placement_idx]
                    
                    if placement_idx == 0:  # 1st place gets a win
                        stats[player["name"]]["wins"] += 1
                        result_lines.append(f"{player['name']} (1st: {player['score']})")
                    elif placement_idx == 1:
                        result_lines.append(f"{player['name']} (2nd: {player['score']})")
                    elif placement_idx == 2:
                        result_lines.append(f"{player['name']} (3rd: {player['score']})")
            
            result_text = " • ".join(result_lines)
            winners = [sorted_participants[0]]  # 1st place player

        scored_pod = {
            "pod_number": pod["pod_number"],
            "players": scored_players,
            "complete": pod_complete,
            "result_text": result_text,
            "tie": tie,
            "winners": winners,
        }
        if pod.get("is_finals"):
            scored_pod["is_finals"] = True
        scored_pods.append(scored_pod)

    return scored_pods, round_complete


def compute_tournament_view(tournament):
    player_names = tournament["player_names"]
    pairing_order = tournament.get("pairing_order") or player_names[:]
    if not tournament.get("pairing_order"):
        shuffle(pairing_order)
        tournament["pairing_order"] = pairing_order
    stats = initialize_stats(player_names)
    rounds = []
    completed_rounds = 0

    for round_number in range(1, 5):
        if round_number == 1:
            ordered_players = pairing_order[:]
        elif round_number == 4:
            # Round 4: Top 4 go to finals (pod 1), rest play in other pods
            ranked = rank_players(stats, player_names)
            finalists = ranked[:4]
            other_players = ranked[4:]
            # Finalists first so they form pod 1
            ordered_players = finalists + other_players
        else:
            ordered_players = rank_players(stats, player_names)

        is_finals = (round_number == 4 and len(player_names) >= 4)
        pods = build_commander_pods(ordered_players, stats, is_finals_round=is_finals)
        
        score_map = tournament["round_scores"].get(round_number, {})
        scored_pods, round_complete = evaluate_round(round_number, pods, score_map, stats, True)

        rounds.append(
            {
                "round_number": round_number,
                "pods": scored_pods,
                "complete": round_complete,
            }
        )

        if round_complete:
            completed_rounds += 1
            continue
        break

    final_order = rank_players(stats, player_names)
    standings = [
        {
            "rank": index,
            "name": name,
            "wins": stats[name]["wins"],
            "points": stats[name]["points"],
            "opponent_match_win_pct": stats[name]["opponent_match_win_pct"],
        }
        for index, name in enumerate(final_order, start=1)
    ]

    status = "complete" if completed_rounds == 4 else "in progress"
    champion_name = standings[0]["name"] if status == "complete" and standings else None

    return {
        "rounds": rounds,
        "completed_rounds": completed_rounds,
        "standings": standings,
        "ready_for_finals": False,
        "finalists": [],
        "finals": None,
        "champion_name": champion_name,
        "status": status,
        "current_round": min(completed_rounds + 1, 4),
    }


def enrich_tournament(tournament, persist=True):
    view = compute_tournament_view(tournament)
    tournament.update(view)
    if persist:
        save_tournament(tournament)
    return tournament


@app.route('/', methods=['GET'])
def index():
    tournaments = list_active_tournaments()
    return render_template(
        'index.html',
        tournaments=tournaments,
        tournament_name='',
        player_count=8,
        player_names_text='',
    )


@app.route('/tournaments/new', methods=['POST'])
def create_tournament_route():
    raw_player_count = request.form.get('player_count', '8')
    try:
        player_count = max(2, min(64, int(raw_player_count)))
    except ValueError:
        player_count = 8

    tournament_name = request.form.get('tournament_name', '').strip()
    raw_player_names = request.form.get('player_names', '')
    player_names = normalize_player_names(raw_player_names, player_count)
    tournament_id = create_tournament(tournament_name, player_count, player_names)
    tournament = enrich_tournament(load_tournament(tournament_id))
    return redirect(url_for('tournament_detail', tournament_id=tournament_id))


@app.route('/tournaments/<int:tournament_id>', methods=['GET', 'POST'])
def tournament_detail(tournament_id):
    tournament = load_tournament(tournament_id)
    current_view = compute_tournament_view(tournament)

    if request.method == 'POST':
        action = request.form.get('action', 'save_round')

        if action == 'save_round':
            round_number = int(request.form.get('round_number', '1'))
            target_round = current_view['rounds'][round_number - 1] if round_number <= len(current_view['rounds']) else None
            if target_round:
                round_scores = build_round_score_map(request.form, round_number, target_round['pods'])
                tournament['round_scores'][round_number] = round_scores

        enrich_tournament(tournament)
        return redirect(url_for('tournament_detail', tournament_id=tournament_id))

    tournament = enrich_tournament(tournament)
    current_round = tournament['current_round']
    active_round = tournament['rounds'][current_round - 1] if current_round <= len(tournament['rounds']) else None

    return render_template(
        'tournament_detail.html',
        tournament=tournament,
        current_round=current_round,
        active_round=active_round,
    )


@app.route('/tournaments/history')
def history():
    tournaments = list_all_tournaments()
    active_tournaments = [t for t in tournaments if t['status'] != 'complete']
    completed_tournaments = [t for t in tournaments if t['status'] == 'complete']
    return render_template(
        'history.html',
        active_tournaments=active_tournaments,
        completed_tournaments=completed_tournaments,
        total_tournaments=len(tournaments),
    )


@app.route('/about')
def about():
    return render_template('about.html')


if __name__ == "__main__":
    app.run(debug=True)

