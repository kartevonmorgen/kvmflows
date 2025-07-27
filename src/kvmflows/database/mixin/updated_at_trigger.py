from typing import TypeVar, Type, Protocol, cast, Any
import peewee_async

from src.kvmflows.database.db import async_db, db


class PeeweeMetadata(Protocol):
    table_name: str
    fields: dict


class ModelWithMeta(Protocol):
    _meta: PeeweeMetadata

    @classmethod
    def create_table(cls, safe: bool = True, **options: Any) -> None: ...


ModelType = TypeVar("ModelType", bound=ModelWithMeta)


class UpdateAtTriggerMixin:
    """A mixin that automatically updates the updated_at field when a model is updated.
    This mixin should only be used with Peewee Model classes that have an updated_at field."""

    @classmethod
    def apply_update_trigger(cls: Type[ModelType]) -> None:
        """Creates or replaces the trigger for updating the updated_at field.

        Raises:
            AttributeError: If the model doesn't have an updated_at field.
        """
        # Check if the model has an updated_at field
        if "updated_at" not in cls._meta.fields:
            raise AttributeError(
                f"Model {cls.__name__} must have an 'updated_at' field to use UpdateAtTriggerMixin"
            )

        table_name = cls._meta.table_name
        trigger_name = f"update_{table_name}_updated_at"
        function_name = f"update_{table_name}_updated_at_fn"

        # Create or replace function with a unique name per table
        db.execute_sql(f"""
        CREATE OR REPLACE FUNCTION {function_name}()
        RETURNS TRIGGER AS $$
        BEGIN
          IF row_to_json(NEW)::text IS DISTINCT FROM row_to_json(OLD)::text THEN
            NEW.updated_at = now();
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """)

        # Drop existing trigger if it exists and create a new one
        db.execute_sql(f"""
        DO $$
        BEGIN
            -- Drop the trigger if it exists
            DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};
            
            -- Create the trigger
            CREATE TRIGGER {trigger_name}
            BEFORE UPDATE ON {table_name}
            FOR EACH ROW EXECUTE PROCEDURE {function_name}();
        END;
        $$;
        """)

    @classmethod
    async def apply_update_trigger_async(cls: Type[ModelType]) -> None:
        """Creates or replaces the trigger for updating the updated_at field asynchronously.

        Raises:
            AttributeError: If the model doesn't have an updated_at field.
        """
        # Check if the model has an updated_at field
        if "updated_at" not in cls._meta.fields:
            raise AttributeError(
                f"Model {cls.__name__} must have an 'updated_at' field to use UpdateAtTriggerMixin"
            )

        table_name = cls._meta.table_name
        trigger_name = f"update_{table_name}_updated_at"
        function_name = f"update_{table_name}_updated_at_fn"

        # Create or replace function with a unique name per table
        await async_db.execute_sql(f"""
        CREATE OR REPLACE FUNCTION {function_name}()
        RETURNS TRIGGER AS $$
        BEGIN
          IF row_to_json(NEW)::text IS DISTINCT FROM row_to_json(OLD)::text THEN
            NEW.updated_at = now();
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """)

        # Drop existing trigger if it exists and create a new one
        await async_db.execute_sql(f"""
        DO $$
        BEGIN
            -- Drop the trigger if it exists
            DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};
            
            -- Create the trigger
            CREATE TRIGGER {trigger_name}
            BEFORE UPDATE ON {table_name}
            FOR EACH ROW EXECUTE PROCEDURE {function_name}();
        END;
        $$;
        """)

    @classmethod
    def create_table_with_trigger(cls, safe: bool = True, **options) -> None:
        """Create the table for this model and apply the updated_at trigger.

        Args:
            safe: If True, the table will only be created if it doesn't exist
            **options: Additional options to pass to the create_table function

        Raises:
            AttributeError: If the model doesn't have an updated_at field.
        """
        with db.atomic():
            # Use database to create table since we can't call super() in a mixin
            # Cast to Any to avoid type checking issues with the mixin
            db.create_tables([cast(Any, cls)], safe=safe)
            cls.apply_update_trigger()
