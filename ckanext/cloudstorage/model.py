#!/usr/bin/env python
# -*- coding: utf-8 -*-
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
import ckan.model as model
from sqlalchemy import (
    Column,
    UnicodeText,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    Index
)
from datetime import datetime
import ckan.model.meta as meta
from ckan.model.domain_object import DomainObject

import logging
log = logging.getLogger(__name__)

Base = declarative_base()
metadata = Base.metadata


def drop_tables():
    metadata.drop_all(model.meta.engine)


def create_tables():
    metadata.create_all(model.meta.engine)


def ensure_tables_exist():
    """
    Create tables only if they don't exist.
    Safe to call during plugin initialization.
    Returns True if any tables were created, False if all already existed.
    """
    try:
        from sqlalchemy import inspect
        inspector = inspect(model.meta.engine)
        existing_tables = inspector.get_table_names()
        
        tables_to_check = [
            'cloudstorage_multipart_part',
            'cloudstorage_multipart_upload', 
            'cloudstorage_azure_upload_status'
        ]
        
        missing_tables = [t for t in tables_to_check if t not in existing_tables]
        
        if missing_tables:
            log.info(f"CloudStorage: Creating missing tables: {missing_tables}")
            # create_all only creates tables that don't exist
            metadata.create_all(model.meta.engine)
            return True
        else:
            log.debug("CloudStorage: All required tables already exist")
            return False
            
    except Exception as e:
        log.warning(f"CloudStorage: Error checking/creating tables: {e}")
        # Try to create anyway - create_all is safe if tables exist
        try:
            metadata.create_all(model.meta.engine)
            return True
        except Exception as e2:
            log.error(f"CloudStorage: Failed to create tables: {e2}")
            return False


class MultipartPart(Base, DomainObject):
    __tablename__ = 'cloudstorage_multipart_part'

    def __init__(self, n, etag, upload):
        self.n = n
        self.etag = etag
        self.upload = upload

    n = Column(Integer, primary_key=True)
    etag = Column(UnicodeText, primary_key=True)
    upload_id = Column(
        UnicodeText, ForeignKey('cloudstorage_multipart_upload.id'),
        primary_key=True
    )
    upload = relationship(
        'MultipartUpload',
        backref=backref('parts', cascade='delete, delete-orphan'),
        single_parent=True)


class MultipartUpload(Base, DomainObject):
    __tablename__ = 'cloudstorage_multipart_upload'

    def __init__(self, id, resource_id, name, size, original_name, user_id):
        self.id = id
        self.resource_id = resource_id
        self.name = name
        self.size = size
        self.original_name = original_name
        self.user_id = user_id

    @classmethod
    def resource_uploads(cls, resource_id):
        query = meta.Session.query(cls).filter_by(
            resource_id=resource_id
        )
        return query

    id = Column(UnicodeText, primary_key=True)
    resource_id = Column(UnicodeText)
    name = Column(UnicodeText)
    initiated = Column(DateTime, default=datetime.utcnow)
    size = Column(Numeric)
    original_name = Column(UnicodeText)
    user_id = Column(UnicodeText)


class AzureUploadStatus(Base, DomainObject):
    """
    Tracks the status of Azure Direct Upload operations.
    Persisted in database to share state between workers and prevent race conditions.
    
    Status values:
    - 'pending': Initial state, upload registered
    - 'moving': Blob is being moved from temp to final location
    - 'completed': Blob successfully moved to final location
    - 'failed': Move operation failed
    """
    __tablename__ = 'cloudstorage_azure_upload_status'
    __table_args__ = (
        Index('ix_azure_upload_temp_path', 'temp_path'),
        Index('ix_azure_upload_resource_id', 'resource_id'),
        Index('ix_azure_upload_status', 'status'),
    )

    def __init__(self, temp_path, resource_id=None, final_path=None, status='pending'):
        self.temp_path = temp_path
        self.resource_id = resource_id
        self.final_path = final_path
        self.status = status
        self.created = datetime.utcnow()
        self.completed = None

    id = Column(Integer, primary_key=True, autoincrement=True)
    temp_path = Column(UnicodeText, unique=True, nullable=False, index=True)
    final_path = Column(UnicodeText, nullable=True)
    resource_id = Column(UnicodeText, nullable=True, index=True)
    status = Column(UnicodeText, default='pending', index=True)
    created = Column(DateTime, default=datetime.utcnow)
    completed = Column(DateTime, nullable=True)

    @classmethod
    def get_by_temp_path(cls, temp_path):
        """Get upload status by temp path."""
        try:
            return meta.Session.query(cls).filter_by(temp_path=temp_path).first()
        except Exception as e:
            log.warning(f"Error querying AzureUploadStatus: {e}")
            return None

    @classmethod
    def get_by_resource_id(cls, resource_id):
        """Get upload status by resource ID."""
        try:
            return meta.Session.query(cls).filter_by(resource_id=resource_id).first()
        except Exception as e:
            log.warning(f"Error querying AzureUploadStatus by resource_id: {e}")
            return None

    @classmethod
    def create_or_lock(cls, temp_path, resource_id, final_path):
        """
        Atomically create a status record or return existing one.
        Returns tuple (status_record, is_new).
        Uses database unique constraint to prevent race conditions.
        """
        try:
            # First check if already exists
            existing = cls.get_by_temp_path(temp_path)
            if existing:
                return existing, False
            
            # Try to create new record
            status = cls(
                temp_path=temp_path,
                resource_id=resource_id,
                final_path=final_path,
                status='moving'
            )
            meta.Session.add(status)
            meta.Session.commit()
            return status, True
            
        except Exception as e:
            meta.Session.rollback()
            # If insert failed due to unique constraint, another worker got there first
            if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                existing = cls.get_by_temp_path(temp_path)
                if existing:
                    return existing, False
            log.warning(f"Error in create_or_lock: {e}")
            return None, False

    @classmethod
    def mark_completed(cls, temp_path, final_path=None):
        """Mark an upload as completed."""
        try:
            status = cls.get_by_temp_path(temp_path)
            if status:
                status.status = 'completed'
                status.completed = datetime.utcnow()
                if final_path:
                    status.final_path = final_path
                meta.Session.commit()
                return True
        except Exception as e:
            meta.Session.rollback()
            log.warning(f"Error marking upload completed: {e}")
        return False

    @classmethod
    def mark_failed(cls, temp_path, error_msg=None):
        """Mark an upload as failed."""
        try:
            status = cls.get_by_temp_path(temp_path)
            if status:
                status.status = 'failed'
                status.completed = datetime.utcnow()
                meta.Session.commit()
                return True
        except Exception as e:
            meta.Session.rollback()
            log.warning(f"Error marking upload failed: {e}")
        return False

    @classmethod
    def cleanup_old_entries(cls, hours=24):
        """Remove completed/failed entries older than specified hours."""
        try:
            from datetime import timedelta
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            deleted = meta.Session.query(cls).filter(
                cls.status.in_(['completed', 'failed']),
                cls.completed < cutoff
            ).delete(synchronize_session=False)
            meta.Session.commit()
            log.info(f"Cleaned up {deleted} old AzureUploadStatus entries")
            return deleted
        except Exception as e:
            meta.Session.rollback()
            log.warning(f"Error cleaning up old entries: {e}")
            return 0

    @classmethod
    def get_pending_temp_paths(cls, hours=24):
        """Get temp paths that are stuck in pending/moving state for too long."""
        try:
            from datetime import timedelta
            cutoff = datetime.utcnow() - timedelta(hours=hours)
            return meta.Session.query(cls).filter(
                cls.status.in_(['pending', 'moving']),
                cls.created < cutoff
            ).all()
        except Exception as e:
            log.warning(f"Error getting pending temp paths: {e}")
            return []
