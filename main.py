import functools
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional


@dataclass(frozen=True)
class TcgRecord:
    raw_source: str
    parsed_data: Optional[dict] = None
    normalized_data: Optional[dict] = None
    is_valid: bool = False
    error_message: Optional[str] = None
    processed_at: Optional[datetime] = None


def parse(raw_json: str) -> TcgRecord:
    try:
        data = json.loads(raw_json)
        return TcgRecord(raw_source=raw_json, parsed_data=data)
    except json.JSONDecodeError as e:
        return TcgRecord(raw_source=raw_json, error_message=f"Invalid JSON: {e}")


def validate(card: TcgRecord) -> TcgRecord:
    if card.error_message:
        return card

    if not card.parsed_data:
        return TcgRecord(
            raw_source=card.raw_source,
            parsed_data=card.parsed_data,
            is_valid=False,
            error_message="No parsed data",
        )

    required_fields = ["game", "name", "set_code", "rarity", "price"]
    missing = [f for f in required_fields if f not in card.parsed_data]

    if missing:
        return TcgRecord(
            raw_source=card.raw_source,
            parsed_data=card.parsed_data,
            is_valid=False,
            error_message=f"Missing required fields: {', '.join(missing)}",
        )

    return TcgRecord(
        raw_source=card.raw_source,
        parsed_data=card.parsed_data,
        is_valid=True,
    )


def normalize(card: TcgRecord) -> TcgRecord:
    if card.error_message:
        return card

    if not card.parsed_data or not card.is_valid:
        return TcgRecord(
            raw_source=card.raw_source,
            parsed_data=card.parsed_data,
            normalized_data=None,
            is_valid=card.is_valid,
        )

    data = card.parsed_data
    game = data["game"].lower().replace("-", "").replace(" ", "")

    if game not in ["pokemon", "yugioh", "magic"]:
        return TcgRecord(
            raw_source=card.raw_source,
            parsed_data=card.parsed_data,
            normalized_data=None,
            is_valid=False,
            error_message=f"Unknown game: {game}",
        )

    rarity = data["rarity"].title()
    price = float(data["price"])
    quantity = int(data.get("quantity", 1))

    game_display = {"pokemon": "Pokemon", "yugioh": "Yu-Gi-Oh", "magic": "Magic"}.get(
        game, game
    )

    normalized = {
        "game": game_display,
        "name": data["name"],
        "set_code": data["set_code"],
        "rarity": rarity,
        "price": price,
        "quantity": quantity,
        "total_value": price * quantity,  # derived field: total card value
    }

    return TcgRecord(
        raw_source=card.raw_source,
        parsed_data=card.parsed_data,
        normalized_data=normalized,
        is_valid=card.is_valid,
    )


def pipeline(
    raw_json: str, transformations: list[Callable[[TcgRecord], TcgRecord]]
) -> TcgRecord:
    card = parse(raw_json)

    if card.error_message:
        return card

    return functools.reduce(
        lambda rec, fn: fn(rec),
        transformations,
        card,
    )


def process_batch(
    raw_json_strings: list[str], transformations: list[Callable[[TcgRecord], TcgRecord]]
) -> tuple[list[TcgRecord], list[TcgRecord]]:
    results = [pipeline(raw, transformations) for raw in raw_json_strings]

    processed = [card for card in results if card.normalized_data is not None]
    failed = [card for card in results if card.normalized_data is None]
    return processed, failed


def main() -> None:
    raw_documents = [
        '{"game": "Pokemon", "name": "Charizard", "set_code": "Base Set", "rarity": "Ultra Rare", "price": "150.00", "quantity": 2}',
        '{"game": "Yu-Gi-Oh", "name": "Dark Magician", "set_code": "LOB", "rarity": "Rare", "price": "25.50", "quantity": 1}',
        '{"game": "Magic", "name": "Black Lotus", "set_code": "Alpha", "rarity": "Rare", "price": "5000.00"}',  # missing quantity, defaults to 1
        '{"game": "Yu-Gi-Oh", "name": "Blue-Eyes White Dragon", "set_code": "SDK", "rarity": "Ultra Rare", "price": "300.00", "quantity": 3}',
        '{"game": "Pokemon", "name": "Pikachu", "set_code": "Base Set", "rarity": "Common", "price": "5.00", "quantity": 10}',
        'not valid json',
        '{"game": "invalid", "name": "Test Card", "set_code": "TEST", "rarity": "Common", "price": "1.00"}',  # unknown game
    ]

    transformations: list[Callable[[TcgRecord], TcgRecord]] = [
        validate,
        normalize,
    ]
    processed, failed = process_batch(raw_documents, transformations)

    print(f"Successfully processed: {len(processed)}")
    for card in processed:
        print(
            f"  - {card.normalized_data['name']} ({card.normalized_data['game'].title()})"
        )
        print(
            f"    Set: {card.normalized_data['set_code']}, Rarity: {card.normalized_data['rarity']}"
        )
        print(
            f"    Price: ${card.normalized_data['price']:.2f} x {card.normalized_data['quantity']} = ${card.normalized_data['total_value']:.2f}"
        )

    print(f"\nFailed: {len(failed)}")
    for card in failed:
        print(f"  - Source: {card.raw_source[:50]}...")
        print(f"    Error: {card.error_message}")


if __name__ == "__main__":
    main()
