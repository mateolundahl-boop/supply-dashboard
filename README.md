# Supply CRM Dashboard

Dashboard interactivo de campanas Supply MX. Se genera desde Redshift y se publica automaticamente en GitHub Pages.

**Link:** https://mateolundahl-boop.github.io/supply-dashboard/

## Setup (una sola vez)

```bash
# 1. Clonar el repo
git clone https://github.com/mateolundahl-boop/supply-dashboard.git
cd supply-dashboard

# 2. Instalar dependencias Python
pip install -r requirements.txt

# 3. Crear archivo .env con tus credenciales de Redshift
cp .env.example .env
# Editar .env con tu usuario y password
```

## Actualizar el dashboard

```bash
cd supply-dashboard
python3 generate_supply_dashboard.py   # Genera index.html (~3 min)
git add index.html
git commit -m "Dashboard update $(date '+%Y-%m-%d')"
git push
```

El link de GitHub Pages se actualiza solo en ~1 minuto despues del push.
