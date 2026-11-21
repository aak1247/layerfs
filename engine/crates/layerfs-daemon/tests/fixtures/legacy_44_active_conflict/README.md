The unmodified SDK ec68fa1fd003fe49343269ea4638e069f779553e created a real two-writer conflict, committed its winner, and retained one settled active conflict at namespace 2. Original owners and Store closed before copying. Tests use private copies.
expected-marker.bin contains the exact marker bytes emitted by that original producer.
