### DESCRIPTION

This file contains a list of test cases and notes on features which should
be covered with automatic tests.

The list is broken down by sections each of which groups tests related to some
functional unit or feature. The format of list is as follows:

#### %Name of a feature or a functional unit%:

* **%Test title%**. %Short description: what this test checks%.
  1. *%Title of test case #1%*. %Brief description of setup, action, and expected
  result%;
  2. *%Title of test case #2%*. `...`

### TESTS

#### Backend:

* **Stat commit errors**. This test checks if backend's `stat_commit_errors` is
  taken into account during `Backend` status recalculation (when errors occur,
  backend must be turned read-only).
  1. *First error*. This test case checks basic functionality. Backend is first
  initialized and enabled, then the status is calculated. It must be `OK`.
  The next `BackendStat` is coming with non-zero `stat_commit_errors`. After the
  status recalculation it must become `RO`.
  2. *Subsequent errors*. This case checks if backend remains in state `RO`
  both when nothing changes and when new errors occur. Send two updates:
  stat with the same number of commit errors and update with greater number
  of errors.
  3. *Restart*. This case checks if `Backend` turns into state `OK` after
  a clean restart. Read-only backend is updated with a stat with more recent
  `last_start` timestamp.
  4. *Reset*. Check if backend becomes `OK` when a new number of commit errors
  is less than previously accounted.
  5. *Read-only*. Repeat tests #3 and #4 with `status/read_only` set to 1. Make
  sure backend remains `RO`.
* **Read-only flag**. This test checks if value of `status/read_only` is taken
  into account. Backend must be turned into state `RO` when the flag is set.
  1. *Flag set*. Check if state transition from `OK` to `RO` works fine. Update
  a backend with `BackendStat` with `read_only` flag set to 1.
  2. *Flag unset*. Check if reverse transition works. Update backend with stat
  with the flag unset.
  3. *Have ROFS errors*. Repeat previous two tests with non-zero
  `stat_commit_errors`. Status must not change and remain `RO`.
* **Group change**. This test verifies that change of a group served by backend
  is correctly handled.
  1. *Backend move*. Basic functionality check: update a backend with a stat
  with `group` changed to another existing. Check if backend and affected
  groups were updated: the backend and the new group are connected, the backend
  is unbound from the old group.
  2. *New group*. Update backend with a stat with group which didn't exist.
  Make sure the new group was created.
* **Stalled backends**. This test verifies status change of backends whose
  description wasn't received for configurable amount of time (default is 120
  seconds). Tests are conducted with timeout set to 1 second.
  1. *Stalled*. Basic functionality test. Update a backend, wait for two
  seconds, recalculate status and make sure it became `STALLED`.
  2. *Resurrection*. Check if resurrection turns backend into state `OK`.
  Update backend from previous test case with fresh stat.
  3. *Disabled backend*. Having backend in state `OK`, change `status/state`
  to `0` (`DNET_BACKEND_DISABLED`). State must become `STALLED`.
* **CommandStat**. This test verifies calculation of disk and net RW rates.
  Information is taken from `DNET_MONITOR_COMMANDS` statistic. Result is checked
  after a new `BackendStat` was passed to `Backend::update`.
  1. *Update #1*. This test checks numbers after the first update received.
  They must not change (remain zeroes).
  2. *Update #2*. This test checks numbers after second update coming 1 minute
  after the first one. Values must be checked according to formulas
  (**TODO**: docs).
  3. *Immediate update*. Make sure update coming less than one second after
  previous one doesn't affect values.
* **Full check**. This test checks `Backend::full()` method. **TODO: docs**.
  1. *Default settings*. No reserved space. Check result with empty, non-empty,
  and full backend.
  2. *Non-zero reserved space*. Repeat calculations with reserved space 0.5.
* **Commands stat**. Verify commands stat calculation (see docs).

#### Couple:

* **DC sharing check**. This test verifies a stage of couple's status
calculation on which pairs of groups are checked for not having node backends
stored in the same DC. All test cases must pass for couples of two and of three
groups.
  1. *Basic setup #1*. This test does straightforward check of a simple setup.
  Given a couple with groups having one backend each, all backends are in
  different DCs, we should get couple's status `OK` as soon as
  `Couple::update_status` completes.
  2. *Basic setup #2*. This test does straightforward check of detection of
  wrong setup. There are two groups in a couple that have backends in the same
  DC. Couple's status should be reported `BROKEN`.
  3. *DHT groups #1*. This test makes sure that several backends in the same DC
  serving a DHT group are not considered as causing misconfig. So the setup
  is the same as in #1 except that one of the groups has several backends stored
  in the same DC.
  4. *DHT groups #2*. The same as previous one, but group backends are spread
  over different DCs.
  5. *DHT groups #3*. This test makes sure that detection of wrong setup still
  works fine for DHT groups. Conflicting backends are checked in all
  combinations of sequence numbers within their groups.
* **Couple status**. This test verifies status calculation for `Couple` (method
  `Couple::update_status`). Each test case checks the resulting status under
  certain conditions. Prerequisite of all cases is that none of previous
  conditions are satisfied.
  1. *Metadata conflict*. A pair of groups has different metadata (namespace,
  type, ...). Result: `BAD`. Check different combinations of conflicting groups.
  2. *Frozen*. Some groups(s) are `FROZEN`. Result: `FROZEN`.
  3. *Unmatched total*. All groups are `COUPLED`, total space is unmatched.
  Result: `BROKEN`.
  4. *Full*. All groups are `COUPLED`, some group(s) are full. Result: `FULL`.
  5. *OK*. All groups are `COUPLED`. Result: `OK`.
  6. *Init #1*. Have group in state `INIT`. Result: `INIT`.
  7. *Broken group*. Have group in state `BROKEN`. Result: `BROKEN`.
  8. *Bad group*. Have group in state `BAD`. Result: `BAD`.
  9. *Read-only group*. Have group in state `RO`, no active job. Result: `BAD`.
  Repeat test for migrating group.
  10. TODO: double check and extend this list after
  https://github.com/yandex/mastermind/issues/31.
  11. TODO: add separate test for `account_job_in_status()` in the same way as
  for DC sharing.
  12. *Job #1*. Group has no enabled backends, but have `PENDING`
  `MOVE_JOB`. Result: `SERVICE_STALLED`.
* **Namespace without settings**. This test checks whether Couple is reported
  `BROKEN` when no namespace settings found.
  1. *Correct setup*. Having couple in namespace which is set up
  `update_status` must set state `OK`.
  2. *Incorrect setup*. Check if couple detects misconfig: having namespace with
  default settings make sure that state is reported as `BROKEN`.
* **Effective space calculation**. Verify `Couple::get_effective_space()`.
  **TODO**: docs.
  1. *Default settings*. Check returned value with namespace with default
  settings.
  2. *Configured namespace*. Repeat check with NS reserved space set to 0.0,
  and to 0.5.
* **Full check**. Verify calculation of `Couple::full()`. Test cases as the
  same as for test **Effective space calculation**.

#### Group:

* **Group type**. This test verifies if type of a group is determined correctly.
  It depends on four conditions: metadata was downloaded or not (`md` / `!md`),
  config option `cache_group_path_prefix` is specified (`conf` / `!conf`), have
  backend with base path beginning with specified cache group path prefix
  (`have_backend` / `!have_backend`), group type in metadata is `cache`
  or other. The result of each test case is calculated type.
  1. *Data #1*. `!md` && `!conf`. Result: `DATA`.
  2. *Unmarked*. `!md` && `conf` && `have_backend`. Result: `UNMARKED`. The test
  is repeated with different number of backends (1 and 3), trying each backend
  having specified prefix.
  3. *Data #2*. `!md` && `conf` && `!have_backend`. Result: `DATA`.
  4. *Cache*. `md` && `type=="cache"`. Result: `CACHE`.
  5. *Data #3*. `md` && `type==""`. Result: `DATA`.
* **Group status**. This test verifies status calculation for `Group` (method
  `Group::update_status`). Each test case checks the resulting status under
  certain conditions. Prerequisite of all cases is that none of previous
  conditions are satisfied.
  1. *No backends*. Group has no backends. Result: `INIT`.
  2. *Forbidden DHT*. DHT groups are forbidden, group has more than one
  backends. Result: `BROKEN`.
  3. *No metadata*. Metadata was not parsed. Result: `INIT`.
  4. *Broken backend*. One or more backends are in state `BROKEN`. Result:
  `BROKEN`. Test should be passed for different backend layouts (number of
  backends, position of broken backend).
  5. *Empty couple*. Group of type `DATA` has empty couple in metadata. Result:
  `INIT`.
  6. *Unbound couple*. Couple is defined, but no `Couple` object was bound to
  the group (`Group::set_couple`). Result: `BAD`.
  7. *Inconsistent couple #1*. Couple has different set of groups than group's
  metadata. Result: `BAD`.
  8. *Inconsistent couple #2*. Some of couple's groups has different couple
  in metadata. Result: `BAD`.
  9. *Empty namespace*. Metadata has empty namespace name. Result: `BAD`.
  10. *Wrong couple*. Group identifier is not present in couple field of
  metadata. Result: `BROKEN`.
  11. *RO backend #1*. Have read-only backend, group is migrating (metadata
  field), active job is bound and its id matches metadata. Result: `MIGRATING`.
  12. *RO backend #2*. Have `RO` backend, group is migrating, no active job.
  Result: `BAD`.
  13. *RO backend #3*. Have `RO` backend, group is migrating, active job id
  doesn't match metadata. Result: `BAD`.
  14. *RO backend #4*. Have `RO` backend, group is not marked as migrating.
  Result: `RO`.
  15. *Other backend state*. Group has backend(s) in state `STALLED` or `INIT`.
  Result: `BAD`.
  16. *Coupled*. If none of conditions above are met, group must be in state
  `COUPLED`.
  17. TODO: double check and extend this list after
  https://github.com/yandex/mastermind/issues/31.
* **Effective space**. Verify calculations of `get_free_space()` and
  `get_effective_space()`. Test cases are the same as for Couple's test
  **Effective space**. **TODO**: docs.
* **History**. Verify applying of `GroupHistoryEntry` coming from history
  database. If some of group backends are absent in history entry, they
  must be removed from Group's set of backends. Actions are performed in
  `Group::apply(const GroupHistoryEntry &)`. Test cases should be checked
  for entries of type `manual` and `job`.
  1. *No changes #1*. Apply entry with the same set of backends. Nothing must
  change.
  2. *No changes #2*. Apply entry with the same set of backends plus new
  backends. Nothing must change.
  3. *One backend removed*. Apply entry with one backend absent. It must
  disappear from the list of Group's backends.
  4. *All backends removed #1*. Apply entry with empty set of backends.
  List of Group's backends must become empty.
  5. *All backends removed #2*. Apply entry with totally different set
  of backends. List of Group's backends must become empty.
  6. *Automatic history entries*. Make sure entries of type `automatic`
  are getting skipped.
* **Couple check (issue #97)**. Example setup: there are groups 5, 11, 12.
  They all have coupling info (5, 11, 12) in metadata. These groups are
  in state `COUPLED`, and couple 5:11:12 is in state `OK`. Then we receive
  metadata of group 10 which is (10, 11, 12). After couple check group 10
  must become `BAD`.

#### Filesystem:

* **CommandStat summation**. This test checks whether aggregate disk and net RW
  rates are correctly calculated for filesystems.
  1. *No updates*. Check initial state. When backends were received only once
  `FS` calculated values of RW rates must be zero.
  2. *Updated status*. Basic functionality check. After series of updates `FS`
  calculated values must be sum for all backends stored on this filesystem.
* **I/O rates**. This test checks calculation of `disk_util`, `disk_util_read`,
  `disk_util_write`, `disk_read_rate`, `disk_write_rate`.
  1. *Update #1*. Basic functionality check. Process two updates with time
  difference of 60 seconds. Check calculated values according to formula
  (**TODO**: docs).
  2. *Immediate update*. Add update with time difference less than 1 second.
  Values must be unchanged.
  3. *Update with errors*. Check whether updates are ignored when `dstat/error`
  or `vfs/error` is non-zero.

#### Node:

* **CommandStat summation**. This test checks whether aggregate disk and net RW
  rates are correctly calculated for nodes. Test cases are the same as in
  Filesystem's test **CommandStat summation**.

#### Namespace:

* **Inconsistent metadata**. Check whether all namespaces are created if groups
  in couple have different namespace in metadata.
* **Storage merge**. Check whether all namespaces are cloned in
  `Storage::merge()`.

#### GroupHistoryEntry:

* **Initialization from BSONObj**. This test checks `GroupHistoryEntry`
  construction. See `GroupHistoryEntry.h` for details of BSONObj format.
  1. *Empty history*. Check object with empty `nodes` and non-zero group id.
  2. *No group id*. Try to construct object from BSON with no group id.
  Exception (`std::runtime_error`) must be thrown.
  3. *One backend*. Try BSON with a single complete backend description.
  4. *Wrong backend*. Try single backend with number of fields lacking.
  Repeat test with fields of wrong type.
  5. *Several backends*. Try BSON with several backends, first all correct,
  then with one incorrect (lacking fields/wrong field type).
  6. *Two and three nodes*. Try 2 and 3 audit records and check whether
  the most recent record is selected.

#### Inventory:
* **TODO**: Primarily tests should cover communication with MongoDB,
  and scheduling.
  **Communication with MongoDB**. Connectivity, object interpretation,
  creating and updating new entries. Entries which were not present in
  cache should be added. Check for duplicates.
* **MongoDB cache update**.
  1. *Failed query*. Find request failed during cache update.
  2. *Timeout*. Inventory worker timeouts during cache update.
  3. *Unavailable*. Inventory is unavailable.
  4. *New value*. Got a new value from inventory worker, put mongo record
  update task into queue.
* **MongoDB record update**.
  1. *Update succeeded*.
  2. *Update failed*.
  **Scheduling**. Check if events are properly scheduled: a) re-collection
  of items b) update of expired records.
* **Round**. Test basic inventory use cases (actions performed during round).
  1. *Cached*. Host is in memory cache.
  2. *Connection error*. Host is not in memory cache, request fails due to
  connection error.
  3. *Timeout*. Host is not in memory cache, request fails by timeout.
  4. *Fetched from cocaine*. Host is not in memory cache, request to cocaine
  succeeds.
* **Cocaine client**. Test should check whether cocaine worker is properly
  requested.
  1. *Cold start*. No cache in MongoDB, first call. Inventory should call
  cocaine.
  2. *Subsequent requests*. Ask for the same host after previous test.
  Application should not call cocaine.
  3. *Cached by MongoDB*. Application should not go to cocaine when host
  information is up-to-date.
  4. *Expired*. DB has a record expired by
  'infrastructure_dc_cache_valid_time'. Application should call cocaine.
