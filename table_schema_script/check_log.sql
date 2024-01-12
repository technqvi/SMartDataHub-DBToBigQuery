 SELECT t.*
                FROM public.models_logging_change t
                WHERE date_created>='2024-01-12 21:00' and content_type_id in (36,37,14,7)
                ORDER BY date_created desc
                LIMIT 501