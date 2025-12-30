import pyautogui
import time

print("Iniciando clic automático cada 30 segundos. Presiona CTRL + C para detener.")

try:
    while True:
        pyautogui.click()     # Clic izquierdo
        print("Clic realizado.")
        time.sleep(30)        # Espera 30 segundos
except KeyboardInterrupt:
    print("\nPrograma detenido por el usuario.")
