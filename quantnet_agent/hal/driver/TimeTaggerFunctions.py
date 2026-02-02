import asyncio
import time
import logging
import yaml
import numpy as np
from datetime import datetime
from pathlib import Path
from enum import Enum
from typing import List, Tuple, Optional, Any

# Optional imports with fallback handling
try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    logging.getLogger(__name__).warning("pandas not available - some features will be limited")

try:
    import TimeTagger

    HAS_TIMETAGGER = True
except ImportError:
    HAS_TIMETAGGER = False
    logging.getLogger(__name__).error("TimeTagger module not available")

log = logging.getLogger(__name__)


class MeasurementType(Enum):
    """Enumeration for different measurement types."""

    COUNT = "count"
    RATE = "rate"
    COINCIDENCE = "coincidence"
    SYNC = "sync"


class TimeTaggerManager:
    """
    Manager class for TimeTagger operations with consolidated measurement functions.

    This class provides a unified interface for all TimeTagger measurements including
    channel counts, count rates, coincidences, and synchronized measurements.
    """

    def __init__(self, device_addr: Optional[str] = None, config_file: Optional[str] = None):
        """
        Initialize TimeTagger manager.

        Args:
            device_addr: Network address of the TimeTagger device
            config_file: Path to YAML configuration file
        """
        self.Inst = None
        self.device_addr = device_addr
        self.config_file = config_file

        # Channel configuration parameters
        self.Chlist: List[int] = []
        self.TriggerLevels: List[float] = []
        self.Deadtimes: List[float] = []
        self.DelayTimes: List[float] = []
        self.DataAcquisitionTime: Optional[float] = None

        self._initialize_device()

    def _initialize_device(self):
        """Initialize the TimeTagger device and load configuration."""
        if not HAS_TIMETAGGER:
            log.error("TimeTagger module not available - cannot initialize device")
            return

        if self.config_file is None and self.device_addr is None:
            log.info("Time tagger does not exist - no configuration provided")
            return

        try:
            # Create TimeTagger instance
            if self.device_addr:
                self.Inst = TimeTagger.createTimeTaggerNetwork(self.device_addr)
                log.info(f"Time tagger connected to {self.device_addr}")
            else:
                self.Inst = TimeTagger.createTimeTagger()
                log.info("Local time tagger connected")

            # Load configuration if provided
            if self.config_file:
                self.load_timetagger_config(self.config_file)

        except Exception as e:
            log.error(f"Failed to initialize TimeTagger: {e}")
            self.Inst = None

    def load_timetagger_config(self, filename: str):
        """
        Load TimeTagger configuration from YAML file.

        Args:
            filename: Path to the YAML configuration file
        """
        try:
            with open(filename, "r") as file:
                config = yaml.safe_load(file)

            # Clear existing configuration
            self.Chlist.clear()
            self.TriggerLevels.clear()
            self.Deadtimes.clear()
            self.DelayTimes.clear()

            # Load channel configuration
            channels_config = config.get("TimeTagger", {}).get("Channels", {})
            for channel_name, channel_data in channels_config.items():
                self.Chlist.append(channel_data["ChannelID"])
                self.TriggerLevels.append(channel_data["TriggerLevel"])
                self.Deadtimes.append(channel_data["Deadtime"])
                self.DelayTimes.append(channel_data["DelayTime"])

            # Load acquisition time
            self.DataAcquisitionTime = config.get("TimeTagger", {}).get("DataAcquisitionTime")

            log.info(
                f"TimeTagger config loaded: {len(self.Chlist)} channels, "
                f"acquisition time: {self.DataAcquisitionTime}s"
            )

        except Exception as e:
            log.error(f"Failed to load TimeTagger config from {filename}: {e}")
            raise

    # =============================================================================
    # Channel Configuration Methods
    # =============================================================================

    def initTTChs(self):
        """Initialize TimeTagger channels with loaded configuration."""
        if not self.Inst:
            raise RuntimeError("TimeTagger not initialized")

        try:
            for i, ch in enumerate(self.Chlist):
                self.Inst.setTriggerLevel(ch, self.TriggerLevels[i])
                self.Inst.setDeadtime(ch, self.Deadtimes[i])
                self.Inst.setInputDelay(ch, self.DelayTimes[i])

            log.info(f"Initialized {len(self.Chlist)} TimeTagger channels")

        except Exception as e:
            log.error(f"Failed to initialize channels: {e}")
            raise

    def TTChangeParams(self, channel: int, param: str, value: float):
        """
        Change parameters for a specific channel.

        Args:
            channel: Channel number
            param: Parameter name ('trigger', 'deadtime', 'delay')
            value: New parameter value
        """
        if not self.Inst:
            raise RuntimeError("TimeTagger not initialized")

        try:
            param_methods = {
                "trigger": self.Inst.setTriggerLevel,
                "deadtime": self.Inst.setDeadtime,
                "delay": self.Inst.setInputDelay,
            }

            if param not in param_methods:
                raise ValueError(f"Unknown parameter: {param}. Available: {list(param_methods.keys())}")

            param_methods[param](channel, value)
            log.debug(f"Set channel {channel} {param} to {value}")

        except Exception as e:
            log.error(f"Failed to change parameter {param} for channel {channel}: {e}")
            raise

    def enableTestSignals(self, Chlist: Optional[List[int]] = None):
        """Enable test signals on specified channels."""
        if not self.Inst:
            raise RuntimeError("TimeTagger not initialized")

        channels = Chlist or self.Chlist
        try:
            for ch in channels:
                self.Inst.setTestSignal(ch, True)
            log.info(f"Test signals enabled for channels: {channels}")
        except Exception as e:
            log.error(f"Error enabling test signals: {e}")
            raise

    def disableTestSignals(self, Chlist: Optional[List[int]] = None):
        """Disable test signals on specified channels."""
        if not self.Inst:
            raise RuntimeError("TimeTagger not initialized")

        channels = Chlist or self.Chlist
        try:
            for ch in channels:
                self.Inst.setTestSignal(ch, False)
            log.info(f"Test signals disabled for channels: {channels}")
        except Exception as e:
            log.error(f"Error disabling test signals: {e}")
            raise

    # =============================================================================
    # Measurement Methods
    # =============================================================================

    async def performChannelMeasurement(
        self,
        measurement_type: MeasurementType,
        Chlist: List[int],
        measurement_time: float = 1.0,
        printout: bool = False,
        timeout: Optional[float] = None,
    ) -> Any:
        """
        Perform channel-based measurements (count or rate).

        Args:
            measurement_type: COUNT or RATE measurement type
            Chlist: List of channels to measure
            measurement_time: Measurement duration in seconds
            printout: Whether to print results to log
            timeout: Optional timeout in seconds

        Returns:
            Measurement data array
        """
        if not self.Inst:
            raise RuntimeError("TimeTagger not initialized")

        if measurement_type not in [MeasurementType.COUNT, MeasurementType.RATE]:
            raise ValueError(f"Invalid measurement type: {measurement_type}")

        if not Chlist:
            raise ValueError("Channel list cannot be empty")

        try:

            async def _perform_measurement():
                # Create appropriate measurement object
                if measurement_type == MeasurementType.COUNT:
                    measurement_obj = TimeTagger.Counter(self.Inst, Chlist, binwidth=measurement_time * 1e12)
                else:  # MeasurementType.RATE
                    measurement_obj = TimeTagger.Countrate(self.Inst, Chlist)

                # Execute measurement
                measurement_obj.startFor(int(measurement_time * 1e12))
                measurement_obj.waitUntilFinished()
                return measurement_obj.getData()

            # Apply timeout if specified
            if timeout is not None:
                data = await asyncio.wait_for(_perform_measurement(), timeout=timeout)
            else:
                data = await _perform_measurement()

            # Print results if requested
            if printout:
                unit = "Counts" if measurement_type == MeasurementType.COUNT else "Count rate (/s)"
                self._print_channel_results(Chlist, data, unit)

            return data

        except asyncio.TimeoutError:
            log.error(f"{measurement_type.value} measurement timed out after {timeout} seconds")
            raise
        except Exception as e:
            log.error(f"Error during {measurement_type.value} measurement: {e}")
            raise

    async def performCoincidenceMeasurement(
        self,
        channel_pairs: List[Tuple[int, int]],
        measurement_time: float = 1.0,
        binwidth: float = 1e3,
        n_bins: int = 100,
        timeout: Optional[float] = None,
    ) -> List[Any]:
        """
        Perform coincidence measurements between channel pairs.

        Args:
            channel_pairs: List of (ch1, ch2) tuples
            measurement_time: Measurement duration in seconds
            binwidth: Bin width in picoseconds
            n_bins: Number of bins
            timeout: Optional timeout in seconds

        Returns:
            List of correlation data arrays
        """
        if not self.Inst:
            raise RuntimeError("TimeTagger not initialized")

        if not channel_pairs:
            raise ValueError("Channel pairs list cannot be empty")

        try:

            async def _perform_measurement():
                correlations = []

                # Initialize all correlation measurements
                for ch1, ch2 in channel_pairs:
                    correlation = TimeTagger.Correlation(self.Inst, ch1, ch2, binwidth=binwidth, n_bins=n_bins)
                    correlations.append(correlation)

                # Start all measurements simultaneously
                for correlation in correlations:
                    correlation.startFor(int(measurement_time * 1e12))

                # Wait for all to finish
                for correlation in correlations:
                    correlation.waitUntilFinished()

                # Collect results
                return [correlation.getData() for correlation in correlations]

            # Apply timeout if specified
            if timeout is not None:
                data = await asyncio.wait_for(_perform_measurement(), timeout=timeout)
            else:
                data = await _perform_measurement()

            log.info(f"Coincidence measurement completed for {len(channel_pairs)} channel pairs")
            return data

        except asyncio.TimeoutError:
            log.error(f"Coincidence measurement timed out after {timeout} seconds")
            raise
        except Exception as e:
            log.error(f"Error during coincidence measurement: {e}")
            raise

    async def performSyncMeasurement(self, filenameWrite: str, Chlist: List[int], timeout: Optional[float] = None):
        """
        Perform synchronized measurement and write to file.

        Args:
            filenameWrite: Output filename
            Chlist: List of channels to measure
            timeout: Optional timeout in seconds
        """
        if not self.Inst:
            raise RuntimeError("TimeTagger not initialized")

        if not filenameWrite:
            raise ValueError("Output filename cannot be empty")

        if not Chlist:
            raise ValueError("Channel list cannot be empty")

        if self.DataAcquisitionTime is None:
            raise ValueError("DataAcquisitionTime not configured")

        try:

            async def _perform_measurement():
                synchronized = TimeTagger.SynchronizedMeasurements(self.Inst)
                log.info(f"Starting synchronized measurement for {self.DataAcquisitionTime}s")

                start_time = time.perf_counter()
                synchronized.startFor(int(self.DataAcquisitionTime * 1e12))
                synchronized.waitUntilFinished()

                elapsed_time = time.perf_counter() - start_time
                log.info(f"Synchronized measurement completed in {elapsed_time:.2f}s")
                log.info(f"Data written to {filenameWrite}")

            # Apply timeout if specified
            if timeout is not None:
                await asyncio.wait_for(_perform_measurement(), timeout=timeout)
            else:
                await _perform_measurement()

        except asyncio.TimeoutError:
            log.error(f"Sync measurement timed out after {timeout} seconds")
            raise
        except Exception as e:
            log.error(f"Error during sync measurement: {e}")
            raise

    # =============================================================================
    # Legacy Synchronous Methods (for backward compatibility)
    # =============================================================================

    def TTSyncMeasure(self, filenameWrite: str, Chlist: List[int]):
        """
        Legacy synchronous version of synchronized measurement.

        Note: This method is synchronous and may block. Consider using
        performSyncMeasurement() for async operations.
        """
        if not self.Inst:
            raise RuntimeError("TimeTagger not initialized")

        if not filenameWrite:
            raise ValueError("Filename for writing the data is not provided")

        if not Chlist:
            raise ValueError("Channel list (Chlist) is not provided")

        try:
            synchronized = TimeTagger.SynchronizedMeasurements(self.Inst)
            log.info(f"Starting synchronized measurement for {self.DataAcquisitionTime}s")

            start_time = time.perf_counter()
            synchronized.startFor(int(self.DataAcquisitionTime * 1e12))
            synchronized.waitUntilFinished()

            elapsed_time = time.perf_counter() - start_time
            log.info(f"Measurement completed in {elapsed_time:.2f}s")
            log.info(f"Data written to {filenameWrite}")

        except Exception as e:
            log.error(f"Error during synchronized measurement: {e}")
            raise

    # =============================================================================
    # Utility Methods
    # =============================================================================

    def _print_channel_results(self, Chlist: List[int], data: Any, unit: str):
        """Print formatted channel measurement results."""
        header_format = "{:<10} {:<18}"
        log.info(header_format.format("Channel", unit))
        log.info("-" * 30)

        for i, ch in enumerate(Chlist):
            row_format = "{:<10} {:<18.2f}"
            log.info(row_format.format(ch, float(data[i])))
        log.info("-" * 30)

    def npSaveData(self, filenameRead: Optional[str] = None, ShowDataTable: bool = False):
        """
        Save or display measurement data using numpy.

        Args:
            filenameRead: Input filename to read data from (if None, uses last measurement)
            ShowDataTable: Whether to display the data table in console

        Returns:
            numpy array of loaded/processed data
        """
        import numpy as np
        import pandas as pd
        from pathlib import Path

        try:
            if filenameRead is not None:
                # Load data from file
                file_path = Path(filenameRead)

                if not file_path.exists():
                    raise FileNotFoundError(f"Data file not found: {filenameRead}")

                # Determine file format and load accordingly
                if file_path.suffix.lower() in [".npy"]:
                    data = np.load(filenameRead)
                    log.info(f"Loaded numpy data from {filenameRead}, shape: {data.shape}")

                elif file_path.suffix.lower() in [".npz"]:
                    data_dict = np.load(filenameRead)
                    data = data_dict[list(data_dict.keys())[0]]  # Get first array
                    log.info(f"Loaded compressed numpy data from {filenameRead}, shape: {data.shape}")

                elif file_path.suffix.lower() in [".csv"]:
                    df = pd.read_csv(filenameRead)
                    data = df.to_numpy()
                    log.info(f"Loaded CSV data from {filenameRead}, shape: {data.shape}")

                elif file_path.suffix.lower() in [".txt", ".dat"]:
                    # Try to load as space/tab delimited text
                    try:
                        data = np.loadtxt(filenameRead)
                        log.info(f"Loaded text data from {filenameRead}, shape: {data.shape}")
                    except ValueError:
                        # If loadtxt fails, try with different delimiters
                        with open(filenameRead, "r") as f:
                            lines = f.readlines()

                        # Parse manually for complex formats
                        parsed_data = []
                        for line in lines:
                            if line.strip() and not line.startswith("#"):
                                # Split by whitespace and convert to float
                                row = [float(x) for x in line.strip().split()]
                                parsed_data.append(row)

                        data = np.array(parsed_data)
                        log.info(f"Parsed text data from {filenameRead}, shape: {data.shape}")

                else:
                    raise ValueError(f"Unsupported file format: {file_path.suffix}")

            else:
                # Generate sample data or use last measurement results
                log.warning("No filename provided, generating sample data")

                # Create sample data based on configured channels
                if self.Chlist:
                    # Simulate measurement data for configured channels
                    num_channels = len(self.Chlist)
                    num_time_points = 1000

                    # Generate time series data
                    time_points = np.linspace(0, 10, num_time_points)  # 10 second measurement
                    data = np.zeros((num_time_points, num_channels + 1))  # +1 for time column

                    data[:, 0] = time_points  # First column is time

                    # Generate simulated count data for each channel
                    for i, ch in enumerate(self.Chlist):
                        # Simulate Poisson-like counting statistics
                        base_rate = 1000 + i * 500  # Different base rates per channel
                        noise = np.random.poisson(base_rate, num_time_points)
                        data[:, i + 1] = noise

                    log.info(f"Generated sample data, shape: {data.shape}")
                else:
                    # Fallback: simple 2D array
                    data = np.random.rand(100, 4)
                    log.info("Generated random sample data")

            # Display data table if requested
            if ShowDataTable:
                self._display_data_table(data, filenameRead)

            # Save processed data with timestamp
            if filenameRead is not None:
                output_filename = self._generate_output_filename(filenameRead)
                self._save_processed_data(data, output_filename)

            return data

        except Exception as e:
            log.error(f"Error in npSaveData: {e}")
            raise

    def _display_data_table(self, data: np.ndarray, source_file: Optional[str] = None):
        """Display data table in a formatted way."""
        try:
            if HAS_PANDAS:
                # Create column names
                if data.ndim == 1:
                    df = pd.DataFrame(data, columns=["Value"])
                elif data.ndim == 2:
                    if data.shape[1] == len(self.Chlist) + 1:
                        # Assume first column is time, rest are channels
                        columns = ["Time"] + [f"Channel_{ch}" for ch in self.Chlist]
                    else:
                        columns = [f"Col_{i}" for i in range(data.shape[1])]
                    df = pd.DataFrame(data, columns=columns)
                else:
                    log.warning(f"Cannot display {data.ndim}D data as table")
                    return

                log.info(f"\n=== Data Table {'from ' + source_file if source_file else ''} ===")
                log.info(f"Shape: {data.shape}")
                log.info(f"Data type: {data.dtype}")

                # Show basic statistics
                log.info("\n=== Basic Statistics ===")
                log.info(f"\n{df.describe()}")

                # Show first and last few rows
                log.info("\n=== First 5 rows ===")
                log.info(f"\n{df.head()}")

                if len(df) > 10:
                    log.info("\n=== Last 5 rows ===")
                    log.info(f"\n{df.tail()}")
            else:
                # Fallback without pandas
                self._display_data_table_simple(data, source_file)

        except Exception as e:
            log.error(f"Error displaying data table: {e}")
            # Fallback to simple display
            self._display_data_table_simple(data, source_file)

    def _display_data_table_simple(self, data: np.ndarray, source_file: Optional[str] = None):
        """Simple data display without pandas."""
        log.info(f"\n=== Data Array {'from ' + source_file if source_file else ''} ===")
        log.info(f"Shape: {data.shape}")
        log.info(f"Data type: {data.dtype}")

        if data.size > 0:
            log.info(f"Min: {np.min(data):.6f}, Max: {np.max(data):.6f}")
            log.info(f"Mean: {np.mean(data):.6f}, Std: {np.std(data):.6f}")

            # Show first few elements
            if data.size <= 20:
                log.info(f"Data:\n{data}")
            else:
                log.info(f"First 10 elements: {data.flat[:10]}")
        else:
            log.info("Data array is empty")

    def _generate_output_filename(self, input_filename: str) -> str:
        """Generate output filename with timestamp."""
        from datetime import datetime

        input_path = Path(input_filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        output_filename = input_path.parent / f"{input_path.stem}_processed_{timestamp}{input_path.suffix}"

        return str(output_filename)

    def _save_processed_data(self, data: np.ndarray, output_filename: str):
        """Save processed data to file."""
        try:
            output_path = Path(output_filename)

            # Create directory if it doesn't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save based on file extension
            if output_path.suffix.lower() == ".npy":
                np.save(output_filename, data)
                log.info(f"Saved numpy data to {output_filename}")

            elif output_path.suffix.lower() == ".npz":
                np.savez_compressed(output_filename, data=data)
                log.info(f"Saved compressed numpy data to {output_filename}")

            elif output_path.suffix.lower() == ".csv":
                # Create column headers
                if data.ndim == 2:
                    if data.shape[1] == len(self.Chlist) + 1:
                        headers = ["Time"] + [f"Channel_{ch}" for ch in self.Chlist]
                    else:
                        headers = [f"Col_{i}" for i in range(data.shape[1])]

                    if HAS_PANDAS:
                        df = pd.DataFrame(data, columns=headers)
                        df.to_csv(output_filename, index=False)
                    else:
                        # Fallback: save with numpy and add header manually
                        header_str = ",".join(headers)
                        np.savetxt(output_filename, data, delimiter=",", header=header_str, comments="")
                else:
                    np.savetxt(output_filename, data, delimiter=",")

                log.info(f"Saved CSV data to {output_filename}")

            elif output_path.suffix.lower() in [".txt", ".dat"]:
                # Save as space-delimited text with header
                header = f"TimeTagger data processed at {datetime.now().isoformat()}\n"
                if data.ndim == 2 and data.shape[1] == len(self.Chlist) + 1:
                    header += "Columns: Time, " + ", ".join([f"Channel_{ch}" for ch in self.Chlist])

                np.savetxt(output_filename, data, delimiter="\t", header=header, comments="# ")
                log.info(f"Saved text data to {output_filename}")

            else:
                # Default to numpy format
                np.save(output_filename.replace(output_path.suffix, ".npy"), data)
                log.info(f"Saved data as numpy format to {output_filename.replace(output_path.suffix, '.npy')}")

            # Also save metadata
            self._save_metadata(data, output_filename)

        except Exception as e:
            log.error(f"Failed to save processed data: {e}")
            raise

    def _save_metadata(self, data: np.ndarray, output_filename: str):
        """Save metadata about the measurement and data."""
        try:
            from datetime import datetime
            import json

            metadata = {
                "timestamp": datetime.now().isoformat(),
                "data_shape": data.shape,
                "data_dtype": str(data.dtype),
                "channels": self.Chlist,
                "trigger_levels": self.TriggerLevels,
                "deadtimes": self.Deadtimes,
                "delay_times": self.DelayTimes,
                "acquisition_time": self.DataAcquisitionTime,
                "device_address": self.device_addr,
                "config_file": self.config_file,
                "data_statistics": {
                    "min": float(np.min(data)) if data.size > 0 else None,
                    "max": float(np.max(data)) if data.size > 0 else None,
                    "mean": float(np.mean(data)) if data.size > 0 else None,
                    "std": float(np.std(data)) if data.size > 0 else None,
                },
            }

            # Save metadata as JSON
            metadata_filename = Path(output_filename).with_suffix(".json")
            with open(metadata_filename, "w") as f:
                json.dump(metadata, f, indent=2)

            log.info(f"Saved metadata to {metadata_filename}")

        except Exception as e:
            log.warning(f"Failed to save metadata: {e}")
