import random
from typing import List, Tuple

Card = str
Hand = List[Card]


def build_deck() -> List[Card]:
    ranks = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
    suits = ["H", "D", "C", "S"]
    deck = [f"{r}{s}" for r in ranks for s in suits]
    random.shuffle(deck)
    return deck


def draw_card(deck: List[Card]) -> Card:
    return deck.pop()


def format_card(card: Card) -> str:
    """Return human-friendly card label, e.g., J♦️, 4♥️."""
    rank, suit = card[:-1], card[-1]
    suit_icon = {"H": "♥", "D": "♦", "C": "♣", "S": "♠"}.get(suit, "?")
    return f"{rank}{suit_icon}"


def hand_value(hand: Hand) -> Tuple[int, bool]:
    total = 0
    aces = 0
    for card in hand:
        rank = card[:-1]
        if rank in ["J", "Q", "K"]:
            total += 10
        elif rank == "A":
            aces += 1
            total += 11
        else:
            total += int(rank)
    # Adjust aces from 11 to 1 as needed
    while total > 21 and aces:
        total -= 10
        aces -= 1
    is_soft = aces > 0  # if any ace still counted as 11
    return total, is_soft


def is_blackjack(hand: Hand) -> bool:
    total, _ = hand_value(hand)
    return len(hand) == 2 and total == 21


def dealer_play(hand: Hand, deck: List[Card]) -> Hand:
    while True:
        total, is_soft = hand_value(hand)
        if total < 17 or (total == 17 and is_soft):
            hand.append(draw_card(deck))
            continue
        break
    return hand


def format_hand(hand: Hand) -> str:
    return ", ".join(format_card(c) for c in hand)
