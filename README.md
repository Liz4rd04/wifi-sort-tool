# wifi-sort

A CLI tool to extract and categorize WiFi data from Kismet `.kismet` database files into organized Excel spreadsheets.

## Features

- Reads Kismet SQLite database files directly
- Extracts all available device data (MAC, SSID, encryption, signal strength, GPS, packets, etc.)
- Sorts devices into 3 tabs:
  - **Client-Named**: Your target networks (matched by patterns)
  - **Non-Client-Named**: Other identified networks (with optional exclusion filter)
  - **Unknown Devices**: Devices with no SSID (harder to identify)
- Supports wildcard pattern matching for SSIDs
- Outputs formatted Excel `.xlsx` files

## Installation

### Requirements

- Python 3.6+
- pandas
- openpyxl

### Debian/Raspberry Pi OS

```bash
sudo apt install python3-pandas python3-openpyxl
```

### Other Systems (pip)

```bash
pip install pandas openpyxl
```

## Usage

### Basic Usage

```bash
python3 wifi-sort.py capture.kismet -o output.xlsx --client client.txt
```

### With Exclude Filter

```bash
python3 wifi-sort.py capture.kismet -o output.xlsx --client client.txt --exclude known.txt
```

### Options

| Option | Description |
|--------|-------------|
| `input` | Input Kismet database file (`.kismet`) |
| `-o, --output` | Output Excel file (default: `output.xlsx`) |
| `--client FILE` | Pattern file for client SSIDs (required) |
| `--exclude FILE` | Pattern file for SSIDs to exclude from tab 2 (optional) |
| `-v, --verbose` | Show detailed output |

## Pattern Files

Pattern files contain one SSID pattern per line. Lines starting with `#` are comments.

### Wildcards

| Pattern | Matches |
|---------|---------|
| `ssid*` | ssid , ssid 2.4, ssid Guest |
| `*xfinity*` | xfinitywifi, Xfinity Mobile |
| `ssid*` | ssid Guest, ssid Wireless, ssid-IoT |
| `MyNetwork` | Exact match only |
| `<empty>` | Hidden networks (no SSID) |

### Example `client.txt`

```
# My client's networks
ssid*
```

### Example `exclude.txt`

```
# Known networks to filter out of tab 2


## Output Columns

| Column | Description |
|--------|-------------|
| MAC | Device MAC address |
| SSID | Network name |
| Type | Device type (Wi-Fi AP, Wi-Fi Client, etc.) |
| Manufacturer | OUI manufacturer lookup |
| Encryption | WPA2/WPA3/PSK/Enterprise/Open |
| Channel | WiFi channel number |
| Frequency_MHz | Frequency (2412, 5180, etc.) |
| RSSI_Last | Last seen signal strength |
| RSSI_Min | Weakest signal seen |
| RSSI_Max | Strongest signal seen |
| Packets_Total | Total packets captured |
| Packets_Data | Data packets captured |
| Data_Size_Bytes | Total data transferred |
| First_Seen | Timestamp first detected |
| Last_Seen | Timestamp last detected |
| Latitude | GPS latitude |
| Longitude | GPS longitude |
| Altitude_m | GPS altitude |

## Example

```bash
# Sort a Kismet capture, identifying "ssid" networks as client
# and filtering out known corporate networks from the Non-Client tab
python3 wifi-sort.py wardriving.kismet \
    -o site_survey.xlsx \
    --client examples/client.txt \
    --exclude examples/exclude.txt \
    -v
```

Output:
```
Extracted 187 WiFi devices
Created site_survey.xlsx:
  Client-Named:     35 devices
  Non-Client-Named: 62 devices
  Unknown Devices:  17 devices
```

## License

MIT License - see [LICENSE](LICENSE)
