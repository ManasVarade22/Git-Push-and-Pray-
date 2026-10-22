
import random
import socket
import time

from protocol import DroneFrame, encode_frame

DESTINATION = ("127.0.0.1", 9999)
FRAME_COUNT = 100
LOSS_COUNT = 30
DUPLICATE_COUNT = 10
REORDER_PAIRS = 10
BATCH_INTERVAL = 2.0


def generate_frame(sequence):
    return DroneFrame(
        drone_id=1,
        sequence=sequence % 65536,
        timestamp=int(time.time()),
        latitude=18.5204 + random.uniform(-0.003, 0.003),
        longitude=73.8567 + random.uniform(-0.003, 0.003),
        sensors=(
            (
                random.randint(1, 2),
                random.randint(1300, 3000),
                random.randint(85, 250),
                random.randint(500, 1500),
                random.randint(80, 200),
                random.randint(500, 1200),
            )
            if random.random() < 0.12
            else (
                random.randint(0, 1),
                random.randint(100, 700),
                random.randint(2, 20),
                random.randint(20, 180),
                random.randint(5, 30),
                random.randint(50, 250),
            )
        ),
    )


def create_batch(start_sequence):
    frames = [
        encode_frame(generate_frame(start_sequence + i))
        for i in range(FRAME_COUNT)
    ]

    dropped_indices = set(
        random.sample(range(FRAME_COUNT), LOSS_COUNT)
    )

    remaining_indices = [
        i for i in range(FRAME_COUNT)
        if i not in dropped_indices
    ]

    duplicate_indices = set(
        random.sample(remaining_indices, DUPLICATE_COUNT)
    )

    send_order = remaining_indices[:]
    pairs = []

    for i in range(0, len(send_order) - 1, 2):
        if len(pairs) >= REORDER_PAIRS:
            break
        pairs.append((send_order[i], send_order[i + 1]))

    for first, second in pairs:
        pos_first = send_order.index(first)
        pos_second = send_order.index(second)
        send_order[pos_first], send_order[pos_second] = (
            send_order[pos_second],
            send_order[pos_first],
        )

    return frames, send_order, duplicate_indices, len(pairs)


def main():
    sequence = 0
    batch_number = 0

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            while True:
                batch_start = time.monotonic()
                frames, send_order, duplicate_indices, pair_count = (
                    create_batch(sequence)
                )

                sent = 0
                duplicates_sent = 0

                for index in send_order:
                    sock.sendto(frames[index], DESTINATION)
                    sent += 1

                    if index in duplicate_indices:
                        sock.sendto(frames[index], DESTINATION)
                        sent += 1
                        duplicates_sent += 1

                    time.sleep(0.02)

                sequence = (sequence + FRAME_COUNT) % 65536
                batch_number += 1

                print(
                    f"Batch {batch_number}: sent {sent} datagrams, "
                    f"duplicates={duplicates_sent}, "
                    f"reordered_pairs={pair_count}"
                )

                elapsed = time.monotonic() - batch_start
                time.sleep(max(0, BATCH_INTERVAL - elapsed))

    except KeyboardInterrupt:
        print("\nContinuous injection stopped.")


if __name__ == "__main__":
    main()
