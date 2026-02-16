# Beurer CosyNight Home Assistant Integration

[![HACS](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/Bibbleq/HA-beurer-cosynight)

Home Assistant integration for Beurer CosyNight heated mattress cover with device grouping and comprehensive controls.

## Features

### Device Controls
- **Body Zone** - Control body zone heating intensity (0-9 levels)
- **Feet Zone** - Control feet zone heating intensity (0-9 levels)
- **Duration** - Set heating duration with a slider (0.5-12 hours in 0.5-hour increments)
- **Remaining Time** - Sensor showing remaining heating time
- **Stop Button** - Instantly stop all heating

### Integration Benefits
- Add Beurer CosyNight devices via Home Assistant UI
- All entities grouped under a single device per blanket
- No YAML configuration required
- Timer changes apply immediately to both heating zones
- Last poll timestamp visible in device entity attributes

## Installation

### HACS (Recommended)
1. Add this repository as a custom repository in HACS
2. Install the integration
3. Restart Home Assistant
4. Go to **Settings → Devices & Services → Add Integration**
5. Search for "Beurer CosyNight"
6. Enter your Beurer CosyNight account credentials

### Manual Installation
1. Copy the `custom_components/beurer_cosynight` folder to your Home Assistant `config/custom_components` directory
2. Restart Home Assistant
3. Follow steps 4-6 above

## Configuration

Setup is done entirely through the Home Assistant GUI. No YAML configuration is required.

### Configuring Polling Behavior (Optional)

After adding the integration, you can configure the polling behavior via the integration's options:

1. Go to **Settings → Devices & Services**
2. Find the Beurer CosyNight integration
3. Click **Configure** (three dots menu)
4. Adjust the following settings:
   - **Peak Hours Start/End**: Define when the blanket is likely to be used (default: 20:00 to 08:00)
   - **Off-Peak Interval**: How often to poll when outside peak hours and blanket is inactive (default: 10 minutes)
   - **Peak Interval**: How often to poll during peak hours when blanket is inactive (default: 5 minutes)
   - **Active Blanket Polling**: Enable/disable aggressive polling when blanket is actively heating (default: enabled)

### Polling Strategy

The integration uses an intelligent three-tier polling strategy to reduce API calls while maintaining responsiveness:

1. **Tier 1 - Off-Peak Hours**: Polls every 10 minutes (configurable) when outside peak hours and blanket is inactive
2. **Tier 2 - Peak Hours**: Polls every 5 minutes (configurable) during peak hours when blanket is inactive
3. **Tier 3 - Active Blanket**: Uses progressive polling when blanket is actively heating:
   - 15 seconds for the first minute after a command
   - 30 seconds for minutes 1-5
   - 60 seconds thereafter until blanket turns off

This dramatically reduces API calls from every 30 seconds to much longer intervals during idle periods, while maintaining responsiveness when the blanket is in use.

## Example Lovelace Card

This integration includes an advanced dashboard card example featuring Mushroom cards with heat-level color coding (blue to red gradient for zones 0-9), active/off status indicators, duration controls with +/- buttons, and last-refreshed timestamps. The example shows a two-blanket side-by-side layout.

![Dashboard Card Example](images/dashboard-card-example.png)

### Required Custom Cards

Before using this card, install these custom integrations from HACS (Frontend):

- <a href="https://github.com/piitaya/lovelace-mushroom">Mushroom Cards</a> - Beautiful, modern card designs
- <a href="https://github.com/thomasloven/lovelace-card-mod">card-mod</a> - CSS styling for cards
- <a href="https://github.com/ofekashery/vertical-stack-in-card">Vertical Stack In Card</a> - Group cards without gaps

### Usage

1. Install the required custom cards listed above via HACS
2. View the full card configuration in [beurer-card-example.yaml](beurer-card-example.yaml)
3. **Important**: Replace the entity names in the example (e.g., `sensor.bens_blanket_remaining_time`) with your own entity names. Find them in **Settings > Devices & Services > Beurer CosyNight**
4. If you have only one blanket, use just one of the two card columns from the example
5. Add the card to your dashboard in YAML mode

## Credits & Attribution

This integration is a fork and derivative work, building upon the contributions of previous developers:

- **Original Author**: [Damon Kohler](https://github.com/damonkohler) - [home-assistant-beurer-cosynight](https://github.com/damonkohler/home-assistant-beurer-cosynight)
- **GUI Setup & Timer Controls**: [Mpercy-Git](https://github.com/Mpercy-Git) - [home-assistant-beurer-cosynight](https://github.com/Mpercy-Git/home-assistant-beurer-cosynight)

### What's New in v3.0.0
- **Intelligent Polling**: DataUpdateCoordinator with three-tier polling strategy dramatically reduces API calls
- **Configurable Intervals**: User-configurable polling intervals for off-peak, peak, and active states
- **Peak Hours Configuration**: Define when the blanket is most likely to be used
- **Active Blanket Detection**: Automatically increases polling frequency when blanket is actively heating
- **Options Flow**: Easy configuration through Home Assistant UI
- **Backward Compatible**: Existing installations work with sensible defaults

### What's New in v2.1.0
- **Flexible Timer Slider**: Duration control now uses a slider allowing custom values from 0.5 to 12 hours in 0.5-hour increments
- **Immediate Timer Application**: Timer changes now apply instantly to both zones without requiring zone temperature adjustments
- **Enhanced Visibility**: Last poll timestamp is now prominently displayed in entity attributes for all entities
- All previous v2.0.0 features included

### What's New in v2.0.0
- Entities grouped under device (improved Home Assistant UI experience)
- Added Duration selector for heating time
- Added Remaining Time sensor
- Added Stop button for quick shutoff
- Config flow integration (no YAML required)
- Improved code structure and maintainability

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

This is a derivative work of the original [home-assistant-beurer-cosynight](https://github.com/damonkohler/home-assistant-beurer-cosynight) by Damon Kohler, also licensed under Apache License 2.0.
