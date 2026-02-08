"""Platform for button entity integration."""
from __future__ import annotations

import logging

from . import beurer_cosynight
from .const import DOMAIN

from homeassistant.components.button import ButtonEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.helpers.update_coordinator import CoordinatorEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    add_entities: AddEntitiesCallback,
) -> None:
    """Set up button entities from a config entry."""
    # Get coordinator and devices
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]
    devices = hass.data[DOMAIN][config_entry.entry_id]["devices"]
    
    if not devices:
        _LOGGER.warning("No devices found for Beurer CosyNight")
        return
    
    entities = []
    for d in devices:
        stop_button = StopButton(coordinator, d, hass)
        refresh_button = RefreshButton(coordinator, d, hass, config_entry)
        
        entities.append(stop_button)
        entities.append(refresh_button)
    
    add_entities(entities)
    _LOGGER.info("Added %d button entities for Beurer CosyNight", len(entities))


class StopButton(CoordinatorEntity, ButtonEntity):
    """Button to stop the massage session."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, device, hass) -> None:
        """Initialize the button."""
        super().__init__(coordinator)
        self._device = device
        self._hass = hass
        self._attr_name = "Stop"
        self._attr_unique_id = f"beurer_cosynight_{device.id}_stop_button"
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

    async def async_press(self) -> None:
        """Stop the massage by setting both zones to 0."""
        try:
            # Get current status from coordinator
            status = self.coordinator.data.get(self._device.id)
            if status is None:
                _LOGGER.error("No status available for device %s", self._device.id)
                return
            
            # Create quickstart with both zones set to 0
            qs = beurer_cosynight.Quickstart(
                bodySetting=0,
                feetSetting=0,
                id=status.id,
                timespan=0
            )
            
            # Send to device
            await self._hass.async_add_executor_job(self.coordinator.hub.quickstart, qs)
            
            # Notify coordinator that a command was sent
            self.coordinator.notify_command_sent()
            
            _LOGGER.debug("Stopped massage session for %s", self._device.name)
            self._attr_available = True
        except Exception as e:
            _LOGGER.error("Failed to stop massage: %s", e)


class RefreshButton(CoordinatorEntity, ButtonEntity):
    """Button to force refresh all entities for this device."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, device, hass, config_entry) -> None:
        """Initialize the button."""
        super().__init__(coordinator)
        self._device = device
        self._hass = hass
        self._config_entry = config_entry
        self._attr_name = "Refresh"
        self._attr_unique_id = f"beurer_cosynight_{device.id}_refresh_button"

    @property
    def device_info(self) -> DeviceInfo:
        """Return device info to link this entity to a device."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.id)},
            name=self._device.name,
            manufacturer="Beurer",
            model="CosyNight",
        )

    async def async_press(self) -> None:
        """Force refresh all entities for this device."""
        try:
            # Request refresh from coordinator
            await self.coordinator.async_request_refresh()
            _LOGGER.debug("Refreshed coordinator data for device %s", self._device.name)
        except Exception as e:
            _LOGGER.error("Failed to refresh entities: %s", e)
