

select p."ISO code" , p."Country and areas" ,Year, over_weight_proportion,stunting_proportion,stunting_actual
from [project].[proportion] p LEFT JOIN [project].[auditMalactual] am ON p."ISO code" = am.ISO_code WHERE cast(p.Year as int)> am.recent_year ;


update [project].[auditMalactual] set recent_year = (select TOP 1 Year FROM [project].[proportion] order by Year desc);

