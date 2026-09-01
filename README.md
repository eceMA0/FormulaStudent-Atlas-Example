# FormulaStudent-Atlas-Example

Example usage of ATLAS Open Streaming for Formula Student University teams.

This repo contains a worked example of getting logged car data from a CSV into **Motion Applied's ATLAS** via Motion Applied's streaming platform.

It is aimed at Formula Student teams who already have data from a logger or ECU and want to analyse it in ATLAS. Fork it, point it at your own logs, and adapt it for your car.

```
git clone https://github.com/eceMA0/FormulaStudent-Atlas-Example.git
cd FormulaStudent-Atlas-Example
```

---

## What kind of data this is for

This example replays **historic runs**: a completed session already saved to disk, not a live feed from the car.

The expected input is a **time-series CSV** — one row per sample, one column per channel, sampled at a **fixed rate**. The included example is a Life Racing ECU export at 500 Hz:

```
C:\...\2025-02-12 11-49-10 plus 0h00m00s F88#20984.LRD
0:00.000 to 3:57.558 at 500Hz

Time, Rad_LL_R02, bpf, bpr, ect1, gear, rpm, tps1, vbat
0.000, ####, ####, ####, ####, NEUTRAL, 0, 11.7, ####
0.002, 44.0, 4.64, 3.90, 77.1, NEUTRAL, 0, 11.7, 9.39
```

Two quirks in this format are handled for you:

- `####` — the logger's "no value recorded" marker. Published as a *missing* sample rather than dropped, so channels stay aligned.
- `NEUTRAL`, `FIRST`, `SECOND`, ... — textual gear names on an otherwise numeric channel. Mapped to `0`, `1`, `2`, etc. (see `GearNames` in `CsvDataSource.cs`).

**This example does not suit** variable-rate or event-based logs, or data where each channel has its own independent timestamps. Those need a different packet structure.

---

## Getting started

1. Set up NuGet access — see **NuGet setup** below.
2. Start Kafka — see **How to setup Kafka** below (`docker compose up -d` from the repo root).
3. Drop your CSV into `WriterCSharp/dataFiles/`.
4. Run it:

```
cd WriterCSharp
dotnet run
```

Or point it at any file:

```
dotnet run -- "C:\path\to\your-run.csv"
```

Kafka and ATLAS must both be running before you start the writer. The broker address is set in `Program.cs` (`localhost:9094` by default) and must match the port published by the Compose file.

---

## Adapting it to your own data

### 1. Your CSV format

`CsvDataSource.cs` does all the parsing, and is the **only file you need to touch** for a different CSV layout. It expects a sample-rate line (`... at 500Hz`) above a header row starting with `Time`.

If your logger differs, adjust:

| What to change | Where |
|---|---|
| Sample rate detection | `ParseFrequency` — the `at <n>Hz` regex |
| Header row detection | `Load` — the search for the line starting `Time` |
| Missing-value marker | `MissingValueToken` (default `"####"`) |
| Non-numeric values | `ParseValue` — handles gear names via the `GearNames` lookup; add your own cases here |
| Delimiter | `SplitRow` — change `','` for tab or semicolon files |

Channel names and count are read from the header automatically. **You do not need to declare your channels anywhere in code** — add or remove CSV columns and everything downstream follows.

### 2. Your team name

Channels appear in ATLAS as `identifier:ApplicationName` — e.g. `rpm:UCLR`. Change the constant at the top of `MockDataWriter.cs`:

```csharp
private const string ApplicationName = "UCLR";   // <- your team abbreviation
```

### 3. Channel descriptions and units

Sensible defaults are derived from the CSV: min/max are scanned from the actual logged values so ATLAS scales each axis correctly.

To add proper descriptions and units, create a JSON file **next to your CSV with the same name**, ending `.metadata.json`:

```
dataFiles/my-run.csv
dataFiles/my-run.metadata.json
```

```json
{
  "rpm": {
    "description": "Engine speed measured by the ECU",
    "units": "rpm",
    "formatString": "%5.0f"
  },
  "ect1": {
    "description": "Engine coolant temperature",
    "units": "°C"
  }
}
```

Keys are **channel names exactly as they appear in the CSV header**. Every field is optional — anything omitted falls back to the CSV-derived value, so you can describe a handful of channels and leave the rest. Available fields are in `ParameterMetadata.cs`: `description`, `units`, `formatString`, `minValue`, `maxValue`, `warningMaxValue`.

On startup the console reports how many channels loaded metadata and warns about any entry that matches no column — useful for catching typos.

> **Note:** ATLAS caches channel configuration against a config ID. This project derives that ID from a hash of the configuration, so edits to your metadata are picked up automatically. You may need to reconnect the session in ATLAS to see them.

---

## How it works

| File | Role |
|---|---|
| `Program.cs` | Loads the CSV and metadata, starts the streaming library, wires everything together |
| `CsvDataSource.cs` | Parses the CSV — **the file to change for a different format** |
| `MockDataWriter.cs` | Creates the session, builds the channel configuration, runs the replay loop |
| `PeriodicPacketGenerator.cs` | Turns CSV rows into timestamped data packets, paced to the logged sample rate |
| `ParameterMetadata.cs` | Shape of the `.metadata.json` file |

The sequence is: create session → publish configuration (channel definitions) → stream data packets → end session. The configuration must be sent **before** the data, otherwise ATLAS receives values for channels it doesn't know about.

---

## NuGet setup

The Motion Applied streaming libraries are published to **GitHub Packages** under [github.com/mat-docs](https://github.com/mat-docs). GitHub Packages requires authentication even for public packages, so you need a token — but a **free personal GitHub account is enough**, with no special organisation access.

**1. Generate a token.** On GitHub, go to **Settings → Developer settings → Personal access tokens → Tokens (classic) → Generate new token (classic)**. Tick **`read:packages`** and nothing else — that's all this project needs, and a read-only token limits the damage if it leaks. Copy it immediately; it won't be shown again.

> It must be a **classic** token. Fine-grained tokens do not currently support reading packages from organisations you're not a member of.

**2. Create your config.** Copy `WriterCSharp/NuGet.Config.template` to `WriterCSharp/NuGet.Config` and fill in your GitHub username and token.

**3. Build.**

```
cd WriterCSharp
dotnet build
```

**Why the config is scoped to this project.** NuGet merges every `NuGet.Config` it finds while walking up the directory tree, and it **validates every configured feed during restore — even ones this project doesn't use**. The `<clear />` in the template drops inherited feeds, so an unrelated private feed you can't authenticate against won't fail this build with a confusing `401`.

### Packages used

| Package | Source | Why |
|---|---|---|
| `MA.DataPlatforms.Streaming.Support.Lib.Core` | GitHub | The main library — session, data-format and packet-writing services |
| `MA.Streaming.Abstraction` | GitHub | Interfaces and config types, including `StreamingApiConfiguration` (your broker address) |
| `MA.Streaming.Core` | GitHub | Implementation, plus `StreamCreationStrategy` controlling stream-to-topic mapping |
| `MA.Streaming.Proto.Core` | GitHub | Generated Protobuf types — `ConfigurationPacket`, `ParameterDefinition`, `PeriodicDataPacket` |
| `Google.Protobuf` | nuget.org | Protobuf runtime providing serialisation; pinned explicitly |

Both sources are needed: the four `MA.*` packages come from GitHub Packages, and `Google.Protobuf` from nuget.org.

---

## How to setup Kafka

The stream is delivered over Kafka, so you need a broker running before you start the writer.
A ready-to-use [`docker-compose.yml`](docker-compose.yml) is included in this repo.

You need [Docker Desktop](https://www.docker.com/products/docker-desktop/). Then, from the repo root:

```
docker compose up -d
```

Check it came up:

```
docker compose ps
```

Both `kafka-broker-1` and `kafka-ui-1` should show as running. You can now browse topics
at <http://localhost:8080>. When you're finished:

```
docker compose down
```

Add `-v` if you also want to wipe the messages: `docker compose down -v`.

### Do I need to edit it?

For most teams, **no** — bring it up as-is and it will work with this example unchanged.
The one number that matters is `9094`, because that is what `Program.cs` connects to:

```csharp
var streamApiConfig = new StreamingApiConfiguration(StreamCreationStrategy.TopicBased, "localhost:9094", []);
```

Only change things if you hit one of these:

| Situation | What to change |
|---|---|
| Port 9094 already in use | Change the left side of `"9094:9094"`, `PLAINTEXT_HOST://localhost:9094`, and the address in `Program.cs` — all three must agree |
| Port 8080 already in use | Change the left side of `"8080:8080"` only, then use the new port in your browser |
| You don't want the web UI | Delete the whole `kafka-ui` service |
| Broker runs on another machine | Replace `localhost` in `Program.cs` with that machine's IP, and set `PLAINTEXT_HOST` to the same address |

### Notes on the config

A few things are worth understanding rather than copying blindly:

- **Three listeners, on purpose.** `9092` is how containers talk to the broker, `9093` is
  KRaft's internal controller channel, and `9094` is the only one exposed to your PC. Your
  writer and ATLAS both use `9094`.
- **`localhost` in `KAFKA_ADVERTISED_LISTENERS` is not cosmetic.** Kafka hands this address
  back to clients and they reconnect to it. If it's wrong, the first connection succeeds and
  everything afterwards fails — a confusing failure worth knowing about in advance.
- **Replication factors are `1`** because this is a single broker. That's fine for development
  but means no redundancy, so don't reuse this file as-is for anything you care about keeping.
- **No authentication.** Every listener is `PLAINTEXT`. Keep this on your own machine or a
  trusted team network, not on a public one.
- **Data is not persisted.** Logs go to `/tmp` inside the container, so `docker compose down -v`
  or removing the container discards messages. For a replay example this is usually what you want.

Two things were removed from the original internal version: a hard-coded `172.22.0.0/16`
subnet with fixed container IPs, and the internal network name. Docker assigns addresses
automatically, and the fixed subnet tends to collide with VPNs and other Compose projects.

---

## How to setup ATLAS to see the stream
1. Navigate to Session Browser
2. Click on the + icon next to "Recorders"
3. Select "Stream Recorder" from the dropdown.
4. Select database engine as "SqlLite" and Stream Server as "Stream Server" from the dropdowns. You do not need to modify anything else.
5. Click Close and then click on the "Start" button to start the stream recorder on the recorders menu.
6. Drag the newly created stream recorder from the recorders menu to the "set" area. You should see the STR icon blink red and then become red. This indicates that the stream recorder is now connected to the stream server and is ready to receive data.
7. Choose your displays on ATLAS, press "P" on your keyboard to bring up the Parameter Browser. Under your group identifier you will be able to see your parameters. Double click on any parameter you want to view them on your displays. 

---

## Associate sessions (optional, not included by default)

This example only writes a single, standalone session. The underlying library also supports
**associate sessions** — extra sessions that get linked to the main one as children — but this
repo doesn't drop in an example, since most teams replaying a single logged run don't need it.

### What an associate session is for

Picture a second process running alongside the main writer: maybe a script computing a
derived channel (tyre temp estimate, lap-time prediction, whatever), or a second data source
recorded at the same time (video timestamps, a second logger). Rather than merging that into the
main writer's own packets, it runs as its own session and gets **linked** to the main session so
ATLAS shows it as related, without the main writer needing to know about it in advance.

The main session doesn't wait for an associate, and the associate doesn't need to exist for the
main writer to work — that's why it's optional here. If it does show up while the main session is
running, it gets linked; if it never shows up, nothing changes.

### How the link works

Two things make a session an "associate" rather than a second unrelated main session:

- **Session type.** The associate creates its session with `type: "AssociateSession"` instead of
  `type: "Session"`. This is what tells ATLAS it's a child in the tree, not a peer.
- **Who owns the link.** Only the *main* session calls `AddAssociateSession(dataSource, mainSessionKey, associateSessionKey)`.
  The associate never calls this itself, and never sets its own `AssociateSessionKeys` — the link
  is one-directional, owned by the main session. If both sides try to own it, ATLAS gets confused
  about which is the parent.

### What's already wired up for you

`MockDataWriter.cs` already contains the main-session side of this, unused unless something shows
up matching it:

- `BeginWatchingForAssociateSessions(sessionInfo)` — called right after the main session starts.
  It subscribes to `sessionManagementService.LiveSessionStarted` and also sweeps
  `GetAllSessions()` once, so a candidate that started slightly before or after this call is still
  caught.
- `TryAssociate(...)` — filters candidates down to sessions that: share the same `DataSource`,
  have `Identifier == AssociateIdentifier` (the constant `"PythonAssociateSession"` near the top
  of the file), aren't historical (i.e. not a leftover from a previous run), and haven't already
  been linked. Matching sessions are linked via `AddAssociateSession`.

### Adding your own associate session

1. In your associate process, create a session with:
   - the **same `dataSource`** as the main writer (`"Default"` in this example)
   - `type: "AssociateSession"`
   - `identifier` matching whatever `AssociateIdentifier` is set to in `MockDataWriter.cs`
     (rename the constant to something clearer for your case if you like — just keep both sides
     in sync)
2. Do **not** call `AddAssociateSession` from the associate, and don't populate its own
   `AssociateSessionKeys` — leave that to the main session.
3. Start it any time before or during the main session's run. `BeginWatchingForAssociateSessions`
   picks it up either way.
4. In ATLAS, the associate session appears nested under the main session in the Session Browser
   once linked.

If you need more than one associate at a time, `TryAssociate` already supports that — it's called
for every candidate session it sees, and `associatedKeys` just prevents the same session being
linked twice.

