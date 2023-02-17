def clear(filename="output.txt"):
    with open("logs\\" + filename, "w") as f:
        f.write("")

def log(message, end='\n', filename="output.txt"):
    if not isinstance(message, str):
        message = str(message)
    
    with open("logs\\" + filename, "a", encoding="utf-8") as f:
        f.write(message+end)