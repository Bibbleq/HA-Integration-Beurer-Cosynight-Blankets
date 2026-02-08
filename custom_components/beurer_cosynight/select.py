"""Platform for select entity integration."""
from __future__ import annotations

import logging

from . import beurer_cosynight
from .const import DOMAIN
import voluptuous as vol

import homeassistant.helpers.config_validation as cv
from homeassistant.components.number import NumberEntity
from homeassistant.components.select import (PLATFORM_SCHEMA, SelectEntity)
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.util import dt as dt_util
from homeassistant.helpers.update_coordinator import CoordinatorEntity

_LOGGER = logging.getLogger(__name__)

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    vol.Required(CONF_USERNAME): cv.string,
    vol.Required(CONF_PASSWORD): cv.string,
})


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    add_entities: AddEntitiesCallback,
) -> None:
    """Set up select entities from a config entry."""
    # Get coordinator and devices
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]
    devices = hass.data[DOMAIN][config_entry.entry_id]["devices"]
    
    if not devices:
        _LOGGER.warning("No devices found for Beurer CosyNight")
        return
    
    # Initialize entity storage for RefreshButton coordination
    entities_key = f"{config_entry.entry_id}_entities"
    hass.data[DOMAIN].setdefault(entities_key, {})
    
    entities = []
    for d in devices:
        body_zone = BodyZone(coordinator, d, hass, config_entry.entry_id)
        feet_zone = FeetZone(coordinator, d, hass, config_entry.entry_id)
        
        entities.append(body_zone)
        entities.append(feet_zone)
        
        # Store entity references for RefreshButton
        hass.data[DOMAIN][entities_key].setdefault(d.id, []).extend([body_zone, feet_zone])
    
    add_entities(entities)
    _LOGGER.info("Added %d select entities for Beurer CosyNight", len(entities))


def setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None
) -> None:
    """Set up the select platform (for YAML config)."""
    username = config[CONF_USERNAME]
    password = config.get(CONF_PASSWORD)
    hub = beurer_cosynight.BeurerCosyNight()

    try:
        hub.authenticate(username, password)
    except Exception as e:
        _LOGGER.error("Could not connect to Beurer CosyNight hub: %s", e)
        return

    entities = []
    for d in hub.list_devices():
        entities.append(BodyZone(hub, d, hass))
        entities.append(FeetZone(hub, d, hass))
    add_entities(entities)


class _Zone(CoordinatorEntity, SelectEntity):

    _attr_has_entity_name = True

    def __init__(self, coordinator, device, name, hass, config_entry_id=None) -> None:
        """Initialize the select entity."""
        super().__init__(coordinator)
        self._device = device
        self._attr_name = name
        self._attr_unique_id = f"beurer_cosynight_{device.id}_{name.lower().replace(' ', '_')}"
        self._config_entry_id = config_entry_id
        self._hass = hass
        self._attr_available = True

    @property
    def device_info(self) -> DeviceInfo:
        """Return device info to link this entity to a device."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.id)},
            name=self._device.name,
            manufacturer="Beurer",
            model="CosyNight",
        )

    @property
    def options(self):
        return [str(x) for x in range(0, 10)]

    def _get_timer_value(self) -> float:
        """Get timer value from the number entity if available."""
        default_timer = 1.0  # Default 1 hour

        if self._config_entry_id:
            entities_key = f"{self._config_entry_id}_entities"
            device_entities = self._hass.data.get(DOMAIN, {}).get(entities_key, {}).get(self._device.id, [])
            for entity in device_entities:
                # Look for the DurationTimer (NumberEntity) specifically,
                # not the DeviceTimer sensor which also has '_timer' in its unique_id
                if isinstance(entity, NumberEntity) and hasattr(entity, '_attr_unique_id') and '_timer' in entity._attr_unique_id:
                    value = entity.native_value
                    # Ensure we have a valid numeric timer value (must be positive)
                    if isinstance(value, (int, float)) and value > 0:
                        return float(value)
                    # If timer value is invalid, use default
                    _LOGGER.warning(
                        "Timer value %s is invalid for device %s, using default of %s hours",
                        value, self._device.id, default_timer
                    )
                    return default_timer
        return default_timer


class BodyZone(_Zone):

    def __init__(self, coordinator, device, hass, config_entry_id=None):
        super().__init__(coordinator, device, 'Body Zone', hass, config_entry_id)

    @property
    def current_option(self):
        status = self.coordinator.data.get(self._device.id)
        if status is None:
            return "0"
        return str(status.bodySetting)

    async def async_select_option(self, option: str) -> None:
        """Update the body zone setting."""
        status = self.coordinator.data.get(self._device.id)
        if status is None:
            return
        
        # Get duration from timer entity if available (timer is now in hours)
        timer_hours = self._get_timer_value()
        timer_seconds = int(timer_hours * 3600)  # Convert hours to seconds
        
        # Use coordinator's batched update method to handle simultaneous zone updates
        await self.coordinator.async_set_zone(
            device_id=self._device.id,
            body_setting=int(option),
            timespan=timer_seconds,
        )


class FeetZone(_Zone):

    def __init__(self, coordinator, device, hass, config_entry_id=None):
        super().__init__(coordinator, device, 'Feet Zone', hass, config_entry_id)

    @property
    def current_option(self):
        status = self.coordinator.data.get(self._device.id)
        if status is None:
            return "0"
        return str(status.feetSetting)

    async def async_select_option(self, option: str) -> None:
        """Update the feet zone setting."""
        status = self.coordinator.data.get(self._device.id)
        if status is None:
            return
        
        # Get duration from timer entity if available (timer is now in hours)
        timer_hours = self._get_timer_value()
        timer_seconds = int(timer_hours * 3600)  # Convert hours to seconds
        
        # Use coordinator's batched update method to handle simultaneous zone updates
        await self.coordinator.async_set_zone(
            device_id=self._device.id,
            feet_setting=int(option),
            timespan=timer_seconds,
        )
