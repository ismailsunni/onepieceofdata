# One Piece of Data

The Data of One Piece

## Set Up

1. Install `virtualenv`

    ```bash
    pip install virtualenv

    ```

2. Create virtual environment

    ```bash
    virtualenv .venv
    ```

3. Activate virtual environment

    ```bash
    source .venv/bin/activate
    ```

4. Install requirements

    ```bash
    pip install -r requirements.txt
    ```

5. Deactivate virtual environment

    ```bash
    deactivate
    ```

## Scripts

All scripts are run from scripts directory

### Scraping chapters

```bash
python ./chapter.py
```

### Scraping volumes

```bash
python ./volumes.py
```
