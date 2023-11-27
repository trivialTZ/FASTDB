from django.urls import path

from . import Snapshots


urlpatterns = [
    path("snapshots_index", Snapshots.index, name="snapshots_index"),
    path("create_new_snapshot", Snapshots.create_new_snapshot, name="create_new_snapshot"),
     path("create_new_processing_version", Snapshots.create_new_processing_version, name="create_new_processing_version"),
    path("edit_processing_version", Snapshots.edit_processing_version, name="edit_processing_version"),
    path("index", Snapshots.index, name="index"),
]

