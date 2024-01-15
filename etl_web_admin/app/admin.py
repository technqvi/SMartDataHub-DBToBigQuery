from django.contrib import admin
from .models import  *
from django.contrib.admin import SimpleListFilter
# Register your models here.

@admin.register(ViewSource)
class ViewSourceAdmin(admin.ModelAdmin):
    actions =None
    list_display = ['id','name']
    search_fields = ['name']

    def has_delete_permission(self, request, obj=None):
        return False

class ViewSourceFilter(SimpleListFilter):
    title = "View Source"
    parameter_name = 'view_source'

    def lookups(self, request, model_admin):
        sourceList = set([c.view_source  for c in model_admin.model.objects.select_related('view_source').all()])
        return [(c.id, c.name) for c in sourceList]

    def queryset(self, request, queryset):
        if self.value():
            try:
                view_source_id = int(self.value())
            except (ValueError):
                return queryset.none()
            else:
                return queryset.filter(view_source__id=view_source_id ) 
    
@admin.register(ETLTransaction)
class ETLTransaction(admin.ModelAdmin):
    actions =None
    list_display = ['trans_datetime','view_source','type','no_rows','is_consistent','is_complete']
    search_fields =['trans_datetime']
    list_filter = (ViewSourceFilter,)

    def has_add_permission(self, request, obj=None):
        return False
    def has_delete_permission(self, request, obj=None):
        return False
    def has_change_permission(self, request, obj=None):
        return  False
    
