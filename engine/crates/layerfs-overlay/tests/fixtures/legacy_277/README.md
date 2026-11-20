These files were produced by unmodified LayerFS SDK commit `277368a810c3edd4d5e5555d846fcf367cb48e0c` using the task fixture `legacy_migration_fixture.rs`.

The schema-41 Workspace has identity `[42; 32]`, namespace 1, one committed file and one acknowledged later dirty write. Its creator closed its Store, Workspace Owner, roster and readers before these bytes were captured. Tests copy both files into a private temporary directory before opening them.
