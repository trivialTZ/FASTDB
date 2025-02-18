from django.urls import path
from rest_framework.authtoken import views

from . import Snapshots
from . import Data


urlpatterns = [
    path("snapshots_index", Snapshots.index, name="snapshots_index"),
    path("create_new_snapshot", Snapshots.create_new_snapshot, name="create_new_snapshot"),
    path("create_new_processing_version", Snapshots.create_new_processing_version, name="create_new_processing_version"),
    path("edit_processing_version", Snapshots.edit_processing_version, name="edit_processing_version"),
    path("index", Snapshots.index, name="index"),
    path("create_new_snapshot_tag", Snapshots.create_new_snapshot_tag, name="create_new_snapshot_tag"),
    path("get_dia_sources", Data.get_dia_sources, name="get_dia_sources"),
    path("get_dia_objects", Data.get_dia_objects, name="get_dia_objects"),
    path("acquire_token",Data.acquire_token, name="acquire_token"),
    path('api-token-auth/', views.obtain_auth_token),
    path('raw_query_long',Data.raw_query_long, name="raw_query_long"),
    path('raw_query_short',Data.raw_query_short, name="raw_query_short"),
    path('store_dia_source_data',Data.store_dia_source_data, name="store_dia_source_data"),
    path('store_ds_pv_ss_data',Data.store_ds_pv_ss_data, name="store_ds_pv_ss_data"),
    path('update_ds_pv_ss_valid_flag',Data.update_ds_pv_ss_valid_flag, name="update_ds_pv_ss_valid_flag"),
    path('update_dia_source_valid_flag',Data.update_dia_source_valid_flag, name="update_dia_source_valid_flag"),
    path('create_new_view', Snapshots.create_new_view, name="create_new_view"),
]

