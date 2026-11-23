from twin.chaos import checkpoint_state, latest_batch


def make_checkpoint(root, offsets, commits):
    for folder, ids in (("offsets", offsets), ("commits", commits)):
        (root / folder).mkdir(parents=True)
        for i in ids:
            (root / folder / str(i)).write_text("")
            (root / folder / f".{i}.crc").write_text("")


def test_latest_batch_ignores_checksum_files(tmp_path):
    make_checkpoint(tmp_path, offsets=[0, 1, 12], commits=[0, 1])
    assert latest_batch(tmp_path / "offsets") == 12


def test_in_flight_batch_is_reported(tmp_path):
    make_checkpoint(tmp_path, offsets=[0, 1, 2], commits=[0, 1])
    assert "in flight" in checkpoint_state(tmp_path)


def test_clean_checkpoint_is_not_in_flight(tmp_path):
    make_checkpoint(tmp_path, offsets=[0, 1], commits=[0, 1])
    assert "in flight" not in checkpoint_state(tmp_path)


def test_missing_checkpoint_is_handled(tmp_path):
    assert latest_batch(tmp_path / "offsets") is None
