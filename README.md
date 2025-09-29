# CPUSPM (Session Processing Manager)

## Description

**CPUSPM** is a demo service wrapper over a stateful process manager.  
It provides a convenient mechanism for processing CPU-bound tasks that run for a long time and require saving intermediate processing results.  

The manager's architecture is optimized for extensibility: adding new task handlers does not require significant changes.

> ⚠️ This project is for demonstration purposes only and is not intended to solve specific business problems. Its purpose is to showcase server application design and implementation skills.

## Implemented processors

Three processors have been implemented so far:

- **Summary Image Processor (SIP)**
Sums up the colors of all images submitted during a session and returns the final image at the end. A WebSocket wrapper has also been implemented for it.

- **Heatmap Image Processor (HIP)**  
  Converts images into a grayscale heatmap. In response to each image, it returns an intermediate result in image format.

- **GrayScale Image Processor (GIP)**  
  An example of working without a session: simple conversion of an image to grayscale.

## Installation and environment setup

Before running, you need to set up the environment:

1. Go to the `src` folder
2. Run the command:
```bash
   pipenv shell
   ```
3. Install dependencies:
```bash
   pipenv install
   ```

## Testing

### Processor tests

The `tests` folder contains a set of tests for checking each processor:

- `si_processor_test.py` - test for SIP  
- `gi_processor_test.py` - test for GIP  
- `hi_processor_test.py` - test for HIP  

Running the test:
```bash
python3 gi_processor_test.py
```

The tests emulate the workflow with the processor and open the results using the system image viewer.
> ⚠️ If a heavy viewer (such as GIMP) is installed by default on your system, opening the results may take a noticeable amount of time.

### Session wrapper (WebSocket) tests

> ⚠️ 
> The web server only works on Linux systems.

To run the SIP test with the WebSocket wrapper:

1. Prepare the environment (see the “Installation” section).
2. Start the web server:
```bash
   gunicorn --bind 0.0.0.0:8090 -w 1 -k main.CustomUvicornWorker main:app
   ```
3. Run the test:
```bash
   python3 sw_si_processor_test.py
   ```

The test establishes a WebSocket connection with the service, transfers images, and displays the processing results in the system viewer.