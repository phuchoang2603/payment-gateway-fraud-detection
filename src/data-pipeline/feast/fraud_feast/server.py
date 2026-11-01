from feast.feature_server import get_app as feast_app
from feast.on_demand_feature_view import OnDemandFeatureView

_transform_dict = OnDemandFeatureView.transform_dict


def _transform_dict_with_full_names(view, inputs):
    values = _transform_dict(view, inputs)
    return {
        **values,
        **{
            f"{view.projection.name_to_use()}__{field.name}": values[field.name]
            for field in view.features
            if field.name in values
        },
    }


def get_app(feature_store):
    # Feast 0.67 filters Python outputs by full names without adding their prefixes.
    OnDemandFeatureView.transform_dict = _transform_dict_with_full_names
    return feast_app(feature_store)
