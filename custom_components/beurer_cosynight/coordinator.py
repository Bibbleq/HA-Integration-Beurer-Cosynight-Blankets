"""DataUpdateCoordinator for Beurer CosyNight integration."""
from __future__ import annotations

import asyncio
import logging
from datetime import time, timedelta
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import (
    DOMAIN,
    CONF_PEAK_HOURS_START,
    CONF_PEAK_HOURS_END,
    CONF_OFFPEAK_INTERVAL_MINUTES,
    CONF_PEAK_INTERVAL_MINUTES,
    CONF_ACTIVE_BLANKET_ENABLED,
    DEFAULT_PEAK_HOURS_START,
    DEFAULT_PEAK_HOURS_END,
    DEFAULT_OFFPEAK_INTERVAL_MINUTES,
    DEFAULT_PEAK_INTERVAL_MINUTES,
    DEFAULT_ACTIVE_BLANKET_ENABLED,
)
from . import beurer_cosynight

_LOGGER = logging.getLogger(__name__)

# Debounce delay to batch simultaneous zone updates (in seconds)
DEBOUNCE_DELAY = 0.1

# Default timer duration in seconds (1 hour = 3600 seconds) when no timer is set
DEFAULT_TIMER_SECONDS = 3600


class BeurerCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Beurer CosyNight data."""

    def __init__(
        self,
        hass: HomeAssistant,
        hub,
        devices: list,
        config_entry,
    ) -> None:
        """Initialize the coordinator."""
        self.hub = hub
        self.devices = devices
        self.config_entry = config_entry
        
        # Track when commands are sent to trigger aggressive polling
        self._last_command_time = None
        self._active_polling_enabled = False
        
        # Pending zone updates for batching (device_id -> {bodySetting, feetSetting, timespan})
        self._pending_updates: dict[str, dict[str, Any]] = {}
        self._debounce_tasks: dict[str, asyncio.Task] = {}
        
        # Get configuration options with defaults
        options = config_entry.options if config_entry.options else {}
        self.peak_hours_start = self._parse_time(
            options.get(CONF_PEAK_HOURS_START, DEFAULT_PEAK_HOURS_START)
        )
        self.peak_hours_end = self._parse_time(
            options.get(CONF_PEAK_HOURS_END, DEFAULT_PEAK_HOURS_END)
        )
        self.offpeak_interval_minutes = options.get(
            CONF_OFFPEAK_INTERVAL_MINUTES, DEFAULT_OFFPEAK_INTERVAL_MINUTES
        )
        self.peak_interval_minutes = options.get(
            CONF_PEAK_INTERVAL_MINUTES, DEFAULT_PEAK_INTERVAL_MINUTES
        )
        self.active_blanket_enabled = options.get(
            CONF_ACTIVE_BLANKET_ENABLED, DEFAULT_ACTIVE_BLANKET_ENABLED
        )
        
        # Initialize with off-peak interval
        initial_interval = timedelta(minutes=self.offpeak_interval_minutes)
        
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=initial_interval,
        )

    def _parse_time(self, time_str: str) -> time:
        """Parse time string in HH:MM format."""
        try:
            hours, minutes = time_str.split(":")
            return time(int(hours), int(minutes))
        except (ValueError, AttributeError):
            _LOGGER.error("Invalid time format: %s, using default", time_str)
            return time(20, 0)  # Default to 8pm

    def _is_in_peak_hours(self, current_time: time) -> bool:
        """Check if current time is within peak hours."""
        start = self.peak_hours_start
        end = self.peak_hours_end
        
        # Handle overnight ranges (e.g., 20:00 to 08:00)
        if start > end:
            return current_time >= start or current_time < end
        else:
            # Same-day range (e.g., 09:00 to 17:00)
            return start <= current_time < end

    def _is_blanket_active(self, device_status) -> bool:
        """Check if blanket is actively heating."""
        if not device_status:
            return False
        
        # Check if timer is running or any zone is heating
        return (
            device_status.timer > 0
            or device_status.bodySetting > 0
            or device_status.feetSetting > 0
        )

    def _get_progressive_active_interval(self) -> timedelta:
        """Get progressive interval for active blanket polling."""
        if not self._last_command_time:
            return timedelta(seconds=60)
        
        time_since_command = dt_util.now() - self._last_command_time
        
        # Progressive intervals: 15s (first minute) → 30s (1-5 minutes) → 60s (after 5 minutes)
        if time_since_command < timedelta(minutes=1):
            return timedelta(seconds=15)
        elif time_since_command < timedelta(minutes=5):
            return timedelta(seconds=30)
        else:
            return timedelta(seconds=60)

    def _calculate_update_interval(self) -> timedelta:
        """Calculate the appropriate update interval based on current state."""
        now = dt_util.now()
        current_time = now.time()
        
        # Check if any blanket is active
        any_active = False
        if self.data:
            for device_id, status in self.data.items():
                if self._is_blanket_active(status):
                    any_active = True
                    break
        
        # Tier 3: Active blanket polling (if enabled)
        if any_active and self.active_blanket_enabled:
            if not self._active_polling_enabled:
                _LOGGER.debug("Entering active blanket polling mode")
                self._active_polling_enabled = True
            interval = self._get_progressive_active_interval()
            _LOGGER.debug("Active blanket detected, using %s interval", interval)
            return interval
        
        # Reset active polling state if blanket is no longer active
        if self._active_polling_enabled and not any_active:
            _LOGGER.debug("Blanket inactive, returning to time-based polling")
            self._active_polling_enabled = False
            self._last_command_time = None
        
        # Tier 2: Peak hours
        if self._is_in_peak_hours(current_time):
            interval = timedelta(minutes=self.peak_interval_minutes)
            _LOGGER.debug("Peak hours detected, using %s interval", interval)
            return interval
        
        # Tier 1: Off-peak hours (default)
        interval = timedelta(minutes=self.offpeak_interval_minutes)
        _LOGGER.debug("Off-peak hours, using %s interval", interval)
        return interval

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from API endpoint."""
        data = {}
        
        for device in self.devices:
            try:
                # Call get_status for each device
                status = await self.hass.async_add_executor_job(
                    self.hub.get_status, device.id
                )
                data[device.id] = status
            except Exception as err:
                _LOGGER.error(
                    "Error fetching data for device %s: %s",
                    device.id,
                    err,
                )
                # Keep old data if available, otherwise mark as unavailable
                if self.data and device.id in self.data:
                    data[device.id] = self.data[device.id]
                else:
                    raise UpdateFailed(f"Error fetching data for device {device.id}: {err}")
        
        # After successful update, recalculate interval for next update
        new_interval = self._calculate_update_interval()
        if new_interval != self.update_interval:
            _LOGGER.debug(
                "Updating coordinator interval from %s to %s",
                self.update_interval,
                new_interval,
            )
            self.update_interval = new_interval
        
        return data

    def notify_command_sent(self) -> None:
        """Notify coordinator that a command was sent to trigger active polling."""
        self._last_command_time = dt_util.now()
        _LOGGER.debug("Command sent, triggering active polling mode")
        
        # Force an immediate update to get fresh status
        self.hass.async_create_task(self.async_request_refresh())

    async def async_set_zone(
        self,
        device_id: str,
        body_setting: int | None = None,
        feet_setting: int | None = None,
        timespan: int | None = None,
    ) -> None:
        """Set zone settings with batching to handle simultaneous updates.
        
        This method collects zone updates within a small time window and sends
        them as a single atomic API call, preventing race conditions when both
        zones are updated simultaneously.
        
        Args:
            device_id: The device ID to update
            body_setting: Optional body zone setting (0-9)
            feet_setting: Optional feet zone setting (0-9)
            timespan: Optional timer duration in seconds
        """
        # Initialize pending updates for this device if needed
        if device_id not in self._pending_updates:
            # Get current status as base
            status = self.data.get(device_id) if self.data else None
            if status is None:
                _LOGGER.error("No status available for device %s", device_id)
                return
            
            self._pending_updates[device_id] = {
                "bodySetting": status.bodySetting,
                "feetSetting": status.feetSetting,
                "timespan": timespan if timespan is not None else (status.timer if status.timer > 0 else DEFAULT_TIMER_SECONDS),
                "id": device_id,
            }
        
        # Update with new values (only if provided)
        if body_setting is not None:
            self._pending_updates[device_id]["bodySetting"] = body_setting
        if feet_setting is not None:
            self._pending_updates[device_id]["feetSetting"] = feet_setting
        if timespan is not None:
            self._pending_updates[device_id]["timespan"] = timespan
        
        # Cancel existing debounce task if any
        if device_id in self._debounce_tasks:
            try:
                self._debounce_tasks[device_id].cancel()
            except Exception:
                pass  # Task may already be completed or cancelled
        
        # Schedule the actual API call after debounce delay
        self._debounce_tasks[device_id] = self.hass.async_create_task(
            self._async_apply_pending_update(device_id)
        )

    async def _async_apply_pending_update(self, device_id: str) -> None:
        """Apply pending updates after debounce delay."""
        try:
            # Wait for debounce delay to collect any additional updates
            await asyncio.sleep(DEBOUNCE_DELAY)
        except asyncio.CancelledError:
            # Task was cancelled, likely due to a new update coming in
            return
        
        # Get and clear pending updates
        pending = self._pending_updates.pop(device_id, None)
        self._debounce_tasks.pop(device_id, None)
        
        if pending is None:
            return
        
        try:
            # Create quickstart command with all pending values
            qs = beurer_cosynight.Quickstart(
                bodySetting=pending["bodySetting"],
                feetSetting=pending["feetSetting"],
                id=pending["id"],
                timespan=pending["timespan"],
            )
            
            _LOGGER.debug(
                "Sending batched update for device %s: body=%d, feet=%d, timespan=%d",
                device_id,
                pending["bodySetting"],
                pending["feetSetting"],
                pending["timespan"],
            )
            
            # Send the quickstart command
            await self.hass.async_add_executor_job(self.hub.quickstart, qs)
            
            # Notify that a command was sent
            self.notify_command_sent()
            
        except Exception as e:
            _LOGGER.error("Failed to apply batched update for device %s: %s", device_id, e)
            raise
