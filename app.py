from flask import Flask, jsonify, request
from flask_restx import Api, Resource, fields
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor


app = Flask(__name__)
CORS(app) 

api = Api(app, title="API Bassins de Production", doc='/swagger')

DB_CONFIG = {
    "host": "localhost",
    "database": "bassins_productions",
    "user": "postgres",
    "password": "postgres"
}


def query_db(query, args=(), one=False):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # cur.execute('SET search_path TO "cameroun", public;')
    cur.execute(query, args)
    rv = cur.fetchall()
    cur.close(); conn.close()
    return (rv[0] if rv else None) if one else rv

def modify_db(query, args=(), one=False):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(query, args)
        rv = cur.fetchall() if cur.description else None  # si RETURNING présent
        conn.commit()  # commit seulement après exécution réussie
    except Exception as e:
        conn.rollback()  # rollback uniquement en cas d'erreur
        raise e  # remonte l'erreur pour que Flask ou l'API puisse la gérer
    finally:
        cur.close()
        conn.close()
    return (rv[0] if rv else None) if one else rv


@api.route('/regions')
class Regions(Resource):
    def get(self):
        return query_db("SELECT DISTINCT adm1_name1 FROM Productions ORDER BY adm1_name1")

@api.route('/departements/<string:reg>')
class Depts(Resource):
    def get(self, reg):
        return query_db("SELECT DISTINCT adm2_name1 AS nom FROM Productions WHERE adm1_name1 = %s ORDER BY adm2_name1", (reg,))

@api.route('/communes/<string:dept>')
class Communes(Resource):
    def get(self, dept):
        return query_db("SELECT DISTINCT adm3_name1 AS nom, adm3_pcode FROM Productions WHERE adm2_name1 = %s ORDER BY adm3_name1", (dept,))


@api.route('/annees')
class Years(Resource):
    def get(self):
        return query_db("SELECT DISTINCT annee FROM Productions ORDER BY annee" )


@api.route('/filieres')
class Filieres(Resource):
    def get(self):
        return query_db("SELECT DISTINCT filiere FROM Productions ORDER BY filiere" )

@api.route('/produits/<string:filiere>')
class Produits(Resource):
    def get(self, filiere):
        if not filiere:
            return {"error": "filiere est requise"}, 400
        return query_db("""SELECT DISTINCT produit AS nom FROM Productions WHERE filiere = %s ORDER BY produit""", (filiere,))

@api.route('/carte/cameroun')
class Country(Resource):
    def get(self):
        sql = f"""
        SELECT json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geom)::json,
            'properties', json_build_object(
                'name', adm3_name1
            )
        ) AS feature
        FROM Productions
        WHERE adm3_name1 IS NOT NULL
        """
        results = query_db(sql)
        geojson = {
            "type": "FeatureCollection",
            "features": [row['feature'] for row in results]
        }
        return jsonify(geojson)


@api.route('/carte/couche-geo')
class CarteFilter(Resource):
    @api.param('annee', 'Année de production', required=False)
    @api.param('filiere', 'ID ou nom de filière', required=False)
    @api.param('produit', 'Nom du produit', required=False)
    @api.param('region', 'Nom de la région', required=False)
    @api.param('dept', 'Nom du département', required=False)
    def get(self):
        # Récupération des paramètres
        annee = request.args.get('annee')
        filiere = request.args.get('filiere')
        produit = request.args.get('produit')
        region = request.args.get('region')
        dept = request.args.get('dept')
        
        # Construction de la requête SQL avec filtres dynamiques
        sql = """
            SELECT json_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(geom)::json,
                'properties', json_build_object(
                    'commune', adm3_name1,
                    'departement', adm2_name1,
                    'region', adm1_name1,
                    'annee', annee,
                    'filiere', filiere,
                    'produit', produit,
                    'tonnage', tonnage
                )
            ) AS feature
            FROM Productions
            WHERE 1=1
        """
        
        params = []
        
        # Application des filtres conditionnels
        if annee:
            sql += " AND annee = %s"
            params.append(annee)
        
        if filiere:
            sql += " AND filiere = %s"
            params.append(filiere)
        
        if produit:
            sql += " AND produit = %s"
            params.append(produit)
        
        if region:
            sql += " AND adm1_name1 = %s"
            params.append(region)
        
        if dept:
            sql += " AND adm2_name1 = %s"
            params.append(dept)
        
        # Exécution de la requête
        try:
            results = query_db(sql, tuple(params))
            
            return jsonify({
                "type": "FeatureCollection",
                "features": [row["feature"] for row in results]
            })
        except Exception as e:
            api.abort(500, f"Erreur lors de la récupération des données: {str(e)}")



@api.route('/carte/couche-geo/<string:zone_name>')
class Carte(Resource):
    @api.param('level', 'commune ou departement')
    def get(self, zone_name):
        level = request.args.get('level', 'commune')  # default = commune

        if level == 'commune':
            col = 'adm3_name1'
        elif level == 'departement':
            col = 'adm2_name1'
        elif level == 'region':
            col = 'adm1_name1'
        else:
            return {"error": "level invalide"}, 400

        sql = f"""
        SELECT json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geom)::json,
            'properties', json_build_object(
                'name', {col}
            )
        ) AS feature
        FROM Productions
        WHERE {col} = %s
        """
        results = query_db(sql, (zone_name,))
        if not results:
            return {"error": "Zone non trouvée"}, 404
        geojson = {
            "type": "FeatureCollection",
            "features": [row['feature'] for row in results]
        }
        return jsonify(geojson)


@api.route('/carte/pancarte-details/<string:commune_name>')
class PancarteDetails(Resource):
    @api.param('annee', 'Année de production (ex: 2022)')
    @api.param('filiere', 'Nom de la filière (ex: Agriculture)')
    def get(self, commune_name):
        """Calcul complet des classements basé sur le nom de la commune"""
        annee = request.args.get('annee')
        filiere = request.args.get('filiere')

        if not annee or not filiere:
            return {"message": "Paramètres annee et filiere requis"}, 400

        # SQL : Calcul des totaux et des rangs par Window Functions
        # On regroupe par pcode pour la précision, mais on filtre par nom à la fin
        query = """
            WITH stats_globales AS (
                SELECT 
                    adm3_pcode, adm3_name1, adm2_name1, adm1_name1,
                    SUM(tonnage) as total_tonnage
                FROM Productions
                WHERE annee = %s AND filiere = %s
                GROUP BY adm3_pcode, adm3_name1, adm2_name1, adm1_name1
            ),
            national_sum AS (
                SELECT SUM(total_tonnage) as somme_pays FROM stats_globales
            ),
            classements AS (
                SELECT 
                    *,
                    RANK() OVER (ORDER BY total_tonnage DESC) as rang_nat,
                    RANK() OVER (PARTITION BY adm1_name1 ORDER BY total_tonnage DESC) as rang_reg,
                    RANK() OVER (PARTITION BY adm2_name1 ORDER BY total_tonnage DESC) as rang_dept,
                    (total_tonnage / (SELECT somme_pays FROM national_sum) * 100) as part_nat
                FROM stats_globales
            )
            SELECT * FROM classements WHERE adm3_name1 = %s
        """
        
        # On exécute la requête principale
        main_stats = query_db(query, (annee, filiere, commune_name), one=True)

        if not main_stats:
            return {"message": f"Aucune donnée pour la commune {commune_name}"}, 404

        # Requête pour la liste détaillée des produits du bassin
        prod_sql = """
            SELECT produit, tonnage 
            FROM Productions 
            WHERE adm3_name1 = %s AND annee = %s AND filiere = %s
            ORDER BY tonnage DESC
        """
        produits = query_db(prod_sql, (commune_name, annee, filiere))

        return {
            "nom": main_stats['adm3_name1'],
            "localisation": {
                "departement": main_stats['adm2_name1'],
                "region": main_stats['adm1_name1'],
                "pcode": main_stats['adm3_pcode']
            },
            "statistiques": {
                "tonnage_total": round(float(main_stats['total_tonnage']), 2),
                "contribution_nationale": f"{round(float(main_stats['part_nat']), 2)}%",
                "rangs": {
                    "national": main_stats['rang_nat'],
                    "regional": main_stats['rang_reg'],
                    "departemental": main_stats['rang_dept']
                }
            },
            "filiere_info": {
                "nom": filiere,
                "annee": annee
            },
            "produits_detail": [
                {"produit": p['produit'], "tonnage": float(p['tonnage'])} 
                for p in produits
            ]
        }

@api.route('/stats/global')
class GlobalStats(Resource):
    @api.param('annee', 'Année (ex: 2022)')
    @api.param('filiere', 'Nom de la filière (ex: Agriculture)')
    @api.param('region', 'Nom de la région (optionnel)')
    @api.param('dept', 'Nom du département (optionnel)')
    def get(self):
        annee = request.args.get('annee')
        filiere = request.args.get('filiere')
        region = request.args.get('region')
        dept = request.args.get('dept')

        if not annee or not filiere:
            return {"message": "Année et Filière obligatoires"}, 400

        # 1. Filtre de base
        where_clauses = ["annee = %s", "filiere = %s"]
        params = [annee, filiere]

        if dept:
            where_clauses.append("adm2_name1 = %s")
            params.append(dept)
        elif region:
            where_clauses.append("adm1_name1 = %s")
            params.append(region)

        where_stmt = " WHERE " + " AND ".join(where_clauses)

        # 2. Requête Top 5 Produits
        top_produits = query_db(f"""
            SELECT produit as label, SUM(tonnage) as valeur 
            FROM Productions {where_stmt}
            GROUP BY produit ORDER BY valeur DESC LIMIT 5
        """, tuple(params))

        # 3. Requête Top 5 Bassins (Communes)
        top_bassins = query_db(f"""
            SELECT adm3_name1 as nom, SUM(tonnage) as valeur 
            FROM Productions {where_stmt}
            GROUP BY adm3_name1 ORDER BY valeur DESC LIMIT 5
        """, tuple(params))

        # 4. Production Totale dans la zone
        total_data = query_db(f"SELECT SUM(tonnage) as total FROM Productions {where_stmt}", tuple(params), one=True)
        total_val = float(total_data['total']) if total_data['total'] else 0

        # 5. Logique de Comparaison Intra-Zone (Dynamique)
        if dept:
            group_col = "adm3_name1" # On compare les communes
            titre = f"Répartition par commune dans le {dept}"
        elif region:
            group_col = "adm2_name1" # On compare les départements
            titre = f"Répartition par département en région {region}"
        else:
            group_col = "adm1_name1" # On compare les régions
            titre = "Répartition nationale par région"

        comparaison = query_db(f"""
            SELECT {group_col} as zone, SUM(tonnage) as valeur 
            FROM Productions {where_stmt}
            GROUP BY {group_col} ORDER BY valeur DESC
        """, tuple(params))

        return {
            "production_totale": total_val,
            "top_5_produits": top_produits,
            "top_5_bassins": top_bassins,
            "comparaison": {
                "titre": titre,
                "donnees": comparaison
            }
        }
    



production_model = api.model('Production', {
    'source_commune': fields.String(required=True, description='Nom de la commune source'),
    'produit': fields.String(required=True, description='Nom du produit'),
    'tonnage': fields.Integer(required=True, description='Tonnage de la production'),
    'annee': fields.Integer(required=True, description='Année de la production'),
    'filiere': fields.String(required=True, description='Filière de la production')
})

@api.route('/productions/add')
class AddProduction(Resource):
    @api.expect(production_model)
    def post(self):
        data = request.get_json()  # Récupère le JSON correctement
        source_commune = data.get('source_commune')
        produit = data.get('produit', 'Ovins')
        tonnage = data.get('tonnage', 500)
        annee = data.get('annee', 2026)
        filiere = data.get('filiere', 'Elevage')

        sql = """
            INSERT INTO Productions (
                geom,
                adm3_name1,
                adm3_pcode,
                adm2_name1,
                adm1_name1,
                produit,
                tonnage,
                annee,
                filiere
            )
            SELECT
                geom,
                adm3_name1,
                adm3_pcode,
                adm2_name1,
                adm1_name1,
                %s,
                %s,
                %s,
                %s
            FROM Productions
            WHERE adm3_name1 = %s
            LIMIT 1
            RETURNING id;
        """
        result = modify_db(sql, (produit, tonnage, annee, filiere, source_commune), one=True)
        if not result:
            return {"error": f"La commune {source_commune} n'existe pas"}, 404

        return {"message": f"Nouvelle production ajoutée pour {source_commune} {result}"}, 201




if __name__ == '__main__':
    app.run(debug=True)

