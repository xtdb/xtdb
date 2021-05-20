package crux.api.query.domain

import crux.api.query.context.QueryContext
import crux.api.underware.pv


data class Query(val sections: List<QuerySection>)

