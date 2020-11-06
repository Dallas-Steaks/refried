import requests
import time

import boto3

# notes on what fields mean:
# https://github.com/Society-for-Internet-Blaseball-Research/blaseball-api-spec/blob/master/game-main.md

team_id = "b024e975-1c4a-4575-8936-a3754a08806a"
game_update_base = "https://api.sibr.dev/chronicler/v1/games/updates?order=desc"
game_list_base = (
    f"https://api.sibr.dev/chronicler/v1/games?order=desc&team={team_id}&season=10"
)

client = boto3.client("dynamodb")


def get_games():
    failures = 0
    trying = True
    while trying and failures < 10:
        try:
            games = requests.get(game_list_base).json()
            trying = False
            if "data" in games:
                for game in games["data"]:
                    yield game["gameId"]
        except:
            failures += 1
            # sibr is being sleepy.
            # let's take a nap and hope they wake up
            time.sleep(1)


def get_updates(game):
    page = ""
    running = True
    failures = 0
    while running and failures < 10:
        url = game_update_base + f"&game={game}&page={page}"
        try:
            data = requests.get(url).json()
            failures = 0
            if "nextPage" in data:
                page = data["nextPage"]
                for update in data["data"]:
                    yield update
            else:
                running = False
        except KeyboardInterrupt:
            raise
        except:
            # sibr is being sleepy.
            # let's take a nap and hope they wake up
            failures += 1
            time.sleep(1)


total_writes = 0
cache_max = 25
update_cache = []


def save_update(update):
    parsed_update = prep_update(update)

    update_cache.append(parsed_update)
    if len(update_cache) == cache_max:
        persist_cache()


def persist_cache():
    global total_writes

    if not update_cache:
        return

    delay = 1
    table_name = "steak_updates"
    request_items = {table_name: [
        {"PutRequest": {"Item": item}} for item in update_cache]}
    while request_items:
        response = client.batch_write_item(RequestItems=request_items)
        request_items = response["UnprocessedItems"]
        if request_items:
            print(f"{len(request_items)} missed, trying again...")
            # dynamo db rate limited
            time.sleep(delay)
            delay *= (delay+1)

    total_writes += len(update_cache)
    update_cache.clear()


def main():
    global total_writes

    last_update = None
    next_id = None
    first_update = None

    games = get_games()
    for game in games:
        for update in get_updates(game):
            if last_update is None:
                last_update = update
            else:
                update["next_id"] = next_id
                save_update(update)
            next_id = update["hash"]
            first_update = update

    if last_update:
        # loop last update around to the first id
        last_update["next_id"] = next_id
        save_update(last_update)
        # purge cache before cloning the first update, so hash isn't overwritten
        persist_cache()
        # set the first update as the current one being displayed
        first_update["hash"] = "current"
        save_update(first_update)
    persist_cache()
    print(total_writes)


def prep_update(update):
    d = update["data"]
    body = {
        "hash": {"S": update["hash"]},
        "timestamp": {"S": update["timestamp"]},
        "gameId": {"S": update["gameId"]},
        "next_id": {"S": update["next_id"]},
        "day": {"N": str(d["day"])},
        "phase": {"N": str(d["phase"])},
        "rules": {"S": d["rules"]},
        "awayTeam": {"S": d["awayTeam"]},
        "homeTeam": {"S": d["homeTeam"]},
        "statsheet": {"S": d["statsheet"]},
        "lastUpdate": {"S": d["lastUpdate"]},
        "terminology": {"S": d["terminology"]},
        "awayTeamName": {"S": d["awayTeamName"]},
        "homeTeamName": {"S": d["homeTeamName"]},
        "awayTeamColor": {"S": d["awayTeamColor"]},
        "awayTeamEmoji": {"S": d["awayTeamEmoji"]},
        "homeTeamColor": {"S": d["homeTeamColor"]},
        "homeTeamEmoji": {"S": d["homeTeamEmoji"]},
        "awayBatterName": {"S": d["awayBatterName"]},
        "homeBatterName": {"S": d["homeBatterName"]},
        "awayPitcherName": {"S": d["awayPitcherName"]},
        "homePitcherName": {"S": d["homePitcherName"]},
        "awayTeamNickname": {"S": d["awayTeamNickname"]},
        "homeTeamNickname": {"S": d["homeTeamNickname"]},
        "awayTeamSecondaryColor": {"S": d["awayTeamSecondaryColor"]},
        "homeTeamSecondaryColor": {"S": d["homeTeamSecondaryColor"]},
        "shame": {"BOOL": d["shame"]},
        "finalized": {"BOOL": d["finalized"]},
        "gameStart": {"BOOL": d["gameStart"]},
        "topOfInning": {"BOOL": d["topOfInning"]},
        "gameComplete": {"BOOL": d["gameComplete"]},
        "isPostseason": {"BOOL": d["isPostseason"]},
        "inning": {"N": str(d["inning"])},
        "season": {"N": str(d["season"])},
        "weather": {"N": str(d["weather"])},
        "awayOdds": {"N": str(d["awayOdds"])},
        "awayOuts": {"N": str(d["awayOuts"])},
        "homeOdds": {"N": str(d["homeOdds"])},
        "homeOuts": {"N": str(d["homeOuts"])},
        "awayBalls": {"N": str(d["awayBalls"])},
        "awayBases": {"N": str(d["awayBases"])},
        "awayScore": {"N": str(d["awayScore"])},
        "homeBalls": {"N": str(d["homeBalls"])},
        "homeBases": {"N": str(d["homeBases"])},
        "homeScore": {"N": str(d["homeScore"])},
        "playCount": {"N": str(d["playCount"])},
        "atBatBalls": {"N": str(d["atBatBalls"])},
        "awayStrikes": {"N": str(d["awayStrikes"])},
        "homeStrikes": {"N": str(d["homeStrikes"])},
        "repeatCount": {"N": str(d["repeatCount"])},
        "seriesIndex": {"N": str(d["seriesIndex"])},
        "atBatStrikes": {"N": str(d["atBatStrikes"])},
        "seriesLength": {"N": str(d["seriesLength"])},
        "halfInningOuts": {"N": str(d["halfInningOuts"])},
        "baserunnerCount": {"N": str(d["baserunnerCount"])},
        "halfInningScore": {"N": str(d["halfInningScore"])},
        "awayTeamBatterCount": {"N": str(d["awayTeamBatterCount"])},
        "homeTeamBatterCount": {"N": str(d["homeTeamBatterCount"])},
    }

    strings = ["awayPitcher", "homePitcher", "awayBatter", "homeBatter"]
    for item in strings:
        body[item] = {"S": d[item] or ""}

    fields = {
        "outcomes": "S",
        "baseRunners": "S",
        "basesOccupied": "N",
        "baseRunnerNames": "S",
    }
    for key, val_type in fields.items():
        if d[key]:
            body[key] = {"L": [{val_type: str(text) for text in d[key]}]}

    return body


if __name__ == "__main__":
    main()
