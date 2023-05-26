# Drawing scripts

### Requirement

```bash
python3.10 -m venv py310
source py310/bin/activate
pip3 install --upgrade pip wheel autopep8 numpy pandas matplotlib SciencePlots ipykernel openpyxl scipy
```

### Script structure

- Each folder is responsible for one figure in the paper

- In each folder

  - `data.xlsx` is all the data needed for drawing the figure, each sheet corresponds to one subplot

  - `plot.py` is the drawing script, when executing, it assumes the `PWD` is the containing folder

  - `plot.pdf` will be generated on a scucessful `plot.py` invocation, e.g. 

  - ```bash
    cd tail-latency
    python3 plot.py
    open plot.pdf
    ```

