@test "bats helper libraries loadable" {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-file
}
