"""Platform for number entity integration."""
from __future__ import annotations

import logging

from .const import DOMAIN

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, UnitOfTime
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.util import dt as dt_util
from homeassistant.helpers.update_coordinator import CoordinatorEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    add_entities: AddEntitiesCallback,
) -> None:
    """Set up number entities from a config entry."""
    # Get coordinator and devices
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]
    devices = hass.data[DOMAIN][config_entry.entry_id]["devices"]
    
    if not devices:
        _LOGGER.warning("No devices found for Beurer CosyNight")
        return
    
    # Initialize entity storage for coordination
    entities_key = f"{config_entry.entry_id}_entities"
    hass.data[DOMAIN].setdefault(entities_key, {})
    
    entities = []
    for d in devices:
        duration_timer = DurationTimer(coordinator, d, hass)
        entities.append(duration_timer)
        
        # Store entity reference for coordination with other entities
        hass.data[DOMAIN][entities_key].setdefault(d.id, []).append(duration_timer)
    
    add_entities(entities)
    _LOGGER.info("Added %d number entities for Beurer CosyNight", len(entities))


class DurationTimer(CoordinatorEntity, NumberEntity):
    """Timer duration number entity (slider in hours)."""

    _attr_has_entity_name = True
    _attr_native_min_value = 0.5
    _attr_native_max_value = 12.0
    _attr_native_step = 0.5
    _attr_native_unit_of_measurement = UnitOfTime.HOURS
    _attr_mode = NumberMode.SLIDER

    def __init__(self, coordinator, device, hass) -> None:
        """Initialize the number entity."""
        super().__init__(coordinator)
        self._device = device
        self._hass = hass
        self._attr_name = "Duration"
        self._attr_unique_id = f"beurer_cosynight_{device.id}_timer"
        self._attr_native_value = 1.0  # Default 1 hour
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
    def native_value(self) -> float:
        """Return the current timer value in hours."""
        return self._attr_native_value

    async def async_set_native_value(self, value: float) -> None:
        """Set the timer duration and apply it immediately to both zones."""
        self._attr_native_value = value
        
        # Convert hours to minutes (API expects minutes)
        timer_minutes = int(value * 60)
        
        # Get current device status from coordinator
        status = self.coordinator.data.get(self._device.id)
        if status is None:
            _LOGGER.error("No status available for device %s", self._device.id)
            return
        
        try:
            # Use coordinator's batched update method for consistency
            # This preserves current zone settings while updating the timer
            await self.coordinator.async_set_zone(
                device_id=self._device.id,
                body_setting=status.bodySetting,
                feet_setting=status.feetSetting,
                timer=timer_minutes,
            )
            
            self._attr_available = True
            
            _LOGGER.info(
                "Timer set to %.1f hours (%d minutes) and applied to device %s",
                value, timer_minutes, self._device.name
            )
        except Exception as e:
            _LOGGER.error("Failed to apply timer change: %s", e)
            raise
