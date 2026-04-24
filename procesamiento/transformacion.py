import pandas as pd
import uuid

def aplicar_transformaciones(almacen_datos):

    # TRANSFORMACIÓN 1: Tabla resumen de sobrevivientes en Titanic
    titanic = almacen_datos['Titanic']
    resumen = titanic.groupby('2urvived').size().reset_index(name='Conteo')
    resumen['2urvived'] = resumen['2urvived'].map({0: 'No sobrevivió', 1: 'Sobrevivió'})
    almacen_datos['resumen_sobrevivientes'] = resumen
    print("✅ T1: Resumen de sobrevivientes creado")

    # TRANSFORMACIÓN 2: Columna UniqueKey en Libros
    libros = almacen_datos['Libros']
    libros['UniqueKey'] = [str(uuid.uuid4()) for _ in range(len(libros))]
    almacen_datos['Libros'] = libros
    print("✅ T2: UniqueKey agregada a Libros")

    # TRANSFORMACIÓN 3: Promedio de temperatura en Clima
    clima = almacen_datos['clima']
    col_temp = [c for c in clima.columns if 'temp' in c.lower()][0]
    promedio = clima[col_temp].mean()
    almacen_datos['promedio_temperatura'] = promedio
    print(f"✅ T3: Promedio de temperatura = {promedio:.2f}")

    # TRANSFORMACIÓN 4: Eliminar pasajeros menores de 10 años
    almacen_datos['Titanic'] = almacen_datos['Titanic'][almacen_datos['Titanic']['Age'] >= 10]
    print("✅ T4: Pasajeros menores de 10 años eliminados")

    return almacen_datos