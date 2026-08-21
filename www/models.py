from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy import desc
from sqlalchemy import func
from sqlalchemy.exc import OperationalError

db = SQLAlchemy()

class Informacje(db.Model):
    __tablename__ = 'informacje'
    id = db.Column(db.Integer, primary_key=True)
    numer = db.Column(db.String, default='Brak')
    kwota_hipoteki = db.Column(db.String, default='Brak')
    ksiega = db.Column(db.String, unique=True, nullable=False)
    identyfikator = db.Column(db.String, default='Brak')
    obreb_ewidencyjny = db.Column(db.String, default='Brak')
    polozenie = db.Column(db.String, default='Brak')
    sposob_korzystania = db.Column(db.String, default='Brak')
    obszar_calej = db.Column(db.String, default='Brak')
    typ = db.Column(db.String, default='Brak')
    ulica = db.Column(db.String, default='Brak')
    adres = db.Column(db.String, default='Brak')
    egzekucja = db.Column(db.Integer, default=0)
    darowizna = db.Column(db.Integer, default=0)
    spadek = db.Column(db.Integer, default=0)
    dziedziczenie = db.Column(db.Integer, default=0)

class Wlasciciele(db.Model):
    __tablename__ = 'wlasciciele'
    id = db.Column(db.Integer, primary_key=True)
    ksiega = db.Column(db.String, nullable=False)
    imie = db.Column(db.String, default='Brak')
    nazwisko = db.Column(db.String, default='Brak')
    pesel = db.Column(db.String, default='Brak')
    wiek = db.Column(db.Integer, default=0)
    udzial = db.Column(db.String, default='Brak')
    nazwa = db.Column(db.String, default='Brak')
    czy_firma = db.Column(db.Boolean, default=False)

class Notatki(db.Model):
    __tablename__ = 'notatki'
    id = db.Column(db.Integer, primary_key=True)
    ksiega = db.Column(db.String, nullable=False)
    notatka = db.Column(db.String, nullable=True)

class Status(db.Model):
    __tablename__ = 'status'
    id = db.Column(db.Integer, primary_key=True)
    ksiega = db.Column(db.String, nullable=False)
    stan = db.Column(db.String, default='analiza')

class ScrapingProgress(db.Model):
    __tablename__ = 'scraping_progress'
    task_id = db.Column(db.Integer, db.ForeignKey('task_queue.id'), primary_key=True)
    last_kw = db.Column(db.String(255), nullable=False)

class TaskQueue(db.Model):
    __tablename__ = 'task_queue'
    id = db.Column(db.Integer, primary_key=True)
    department_code = db.Column(db.String(10), nullable=False)
    start_from = db.Column(db.Integer, default=0, nullable=False)
    end_at = db.Column(db.Integer, default=999999, nullable=False)
    priority = db.Column(db.Integer, default=0)  # Higher number = higher priority
    # dead_letter means processing failed after all three retries.
    status = db.Column(db.String(20), default='pending')  # pending, in_progress, stopping, stopped, completed, dead_letter
    books_total = db.Column(db.Integer, default=0)
    books_processed = db.Column(db.Integer, default=0)
    last_scraped_book = db.Column(db.String(50), nullable=True)  # Stores the last book number that was scraped
    date_created = db.Column(db.DateTime, default=db.func.now())
    date_updated = db.Column(db.DateTime, default=db.func.now(), onupdate=db.func.now())

    # Add one-to-one relationship to ScrapingProgress
    scraping_progress = db.relationship('ScrapingProgress', backref='task', uselist=False, cascade='all, delete-orphan')

    @classmethod
    def get_next_pending_task(cls):
        """Get the highest priority pending task"""
        return cls.query.filter_by(status='pending').order_by(desc(cls.priority), cls.date_created).first()

    @classmethod
    def claim_next_pending_task(cls):
        """Atomically reserve one task so competing managers cannot duplicate it."""
        task = (
            cls.query.filter_by(status='pending')
            .order_by(desc(cls.priority), cls.date_created)
            .with_for_update(skip_locked=True)
            .first()
        )
        if task is not None:
            task.status = 'in_progress'
            task.books_total = task.end_at - task.start_from + 1
            db.session.commit()
        else:
            db.session.rollback()
        return task
    
    def __repr__(self):
        return f'<TaskQueue {self.id} | Department: {self.department_code} | Status: {self.status}>'

class TaskStatus(db.Model):
    __tablename__ = 'task_status'
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.String(50), unique=True, nullable=False)
    department_code = db.Column(db.String(10), nullable=False)
    status = db.Column(db.String(20), default='pending')
    date_created = db.Column(db.DateTime, default=db.func.now())
    date_updated = db.Column(db.DateTime, default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f'<Task {self.task_id} | Department: {self.department_code} | Status: {self.status}>'

class Proxy(db.Model):
    __tablename__ = 'proxies'
    id = db.Column(db.String(64), primary_key=True)  # SHA1 hash of proxy URL
    host = db.Column(db.String(255), nullable=False)
    port = db.Column(db.String(10), nullable=False)
    username = db.Column(db.String(255), nullable=True)
    password = db.Column(db.String(255), nullable=True)
    in_use = db.Column(db.Boolean, default=False)
    call_count = db.Column(db.Integer, default=0) 
    failure_count = db.Column(db.Integer, default=0)
    cookies_valid = db.Column(db.Boolean, default=False)
    is_direct = db.Column(db.Boolean, default=False)  # Flag for direct connections (testing)
    cookies = db.Column(JSON, default=list)  # Store cookies as JSON

    def __repr__(self):
        return f'<Proxy {self.host}:{self.port}>'

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    login = db.Column(db.String(64), unique=True, nullable=False)
    password_hash = db.Column(db.String(512), nullable=False)
    is_admin = db.Column(db.Boolean, default=False, nullable=False)
    
    # Relationship to prefix permissions
    prefix_permissions = db.relationship('UserPrefixPermission', backref='user', lazy='select', cascade='all, delete-orphan')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    
    def is_administrator(self):
        return self.is_admin
    
    def get_allowed_prefixes(self):
        """Get list of prefixes this user can access"""
        if self.is_admin:
            # Admins can access all prefixes
            return None  # None means all prefixes
        return [p.prefix for p in self.prefix_permissions]
    
    def has_prefix_access(self, prefix):
        """Check if user has access to a specific prefix"""
        if self.is_admin:
            return True
        return any(p.prefix == prefix for p in self.prefix_permissions)

class UserPrefixPermission(db.Model):
    __tablename__ = 'user_prefix_permissions'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    prefix = db.Column(db.String(4), nullable=False)
    date_created = db.Column(db.DateTime, default=db.func.now())
    
    # Ensure unique user-prefix combinations
    __table_args__ = (db.UniqueConstraint('user_id', 'prefix'),)
    
    def __repr__(self):
        return f'<UserPrefixPermission user_id={self.user_id} prefix={self.prefix}>'

class Egzekucje(db.Model):
    __tablename__ = 'egzekucje'
    id = db.Column(db.Integer, primary_key=True)
    ksiega = db.Column(db.String, nullable=False)
    data_wszczecia = db.Column(db.Date, nullable=False)
    date_created = db.Column(db.DateTime, default=db.func.now())
    
    def __repr__(self):
        return f'<Egzekucja {self.ksiega} | Data: {self.data_wszczecia}>'

class Hipoteki(db.Model):
    __tablename__ = 'hipoteki'
    id = db.Column(db.Integer, primary_key=True)
    ksiega = db.Column(db.String, nullable=False)
    data_wydania = db.Column(db.Date, nullable=False)
    date_created = db.Column(db.DateTime, default=db.func.now())
    
    def __repr__(self):
        return f'<Hipoteka {self.ksiega} | Data: {self.data_wydania}>'

class Spadki(db.Model):
    __tablename__ = 'spadki'
    id = db.Column(db.Integer, primary_key=True)
    ksiega = db.Column(db.String, nullable=False)
    data_orzeczenia = db.Column(db.Date, nullable=False)
    date_created = db.Column(db.DateTime, default=db.func.now())
    
    def __repr__(self):
        return f'<Spadek {self.ksiega} | Data: {self.data_orzeczenia}>'

class Dziedziczenia(db.Model):
    __tablename__ = 'dziedziczenia'
    id = db.Column(db.Integer, primary_key=True)
    ksiega = db.Column(db.String, nullable=False)
    data_orzeczenia = db.Column(db.Date, nullable=False)
    date_created = db.Column(db.DateTime, default=db.func.now())
    
    def __repr__(self):
        return f'<Dziedziczenie {self.ksiega} | Data: {self.data_orzeczenia}>'

class Darowizny(db.Model):
    __tablename__ = 'darowizny'
    id = db.Column(db.Integer, primary_key=True)
    ksiega = db.Column(db.String, nullable=False)
    data_umowy = db.Column(db.Date, nullable=False)
    date_created = db.Column(db.DateTime, default=db.func.now())
    
    def __repr__(self):
        return f'<Darowizna {self.ksiega} | Data: {self.data_umowy}>'


class IdleStatus(db.Model):
    __tablename__ = 'idle_status'
    id = db.Column(db.Integer, primary_key=True, default=1)
    enabled = db.Column(db.Boolean, default=True, nullable=False)
    active = db.Column(db.Boolean, default=False, nullable=False)
    current_code = db.Column(db.String(10), nullable=True)
    current_number = db.Column(db.Integer, nullable=True)
    empty_streak = db.Column(db.Integer, default=0, nullable=False)
    maintenance_mode = db.Column(db.Boolean, default=False, nullable=False)
    last_maintenance_check = db.Column(db.DateTime, nullable=True)
    updated_at = db.Column(db.DateTime, server_default=func.now(), onupdate=func.now())

    @classmethod
    def get_singleton(cls):
        try:
            obj = cls.query.get(1)
        except OperationalError:
            # Table might not exist yet; create all and retry
            try:
                db.create_all()
            except Exception:
                db.session.rollback()
            obj = cls.query.get(1)

        if not obj:
            obj = cls(id=1)
            db.session.add(obj)
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
        return obj


class IdleTasks(db.Model):
    __tablename__ = 'idle_tasks'
    id = db.Column(db.Integer, primary_key=True)
    kod_wydzialu = db.Column(db.String(10), nullable=False)
    # Represent a range of numbers to process for this court
    start_from = db.Column(db.Integer, nullable=False, default=0)
    end_at = db.Column(db.Integer, nullable=False, default=999999)
    # Track progress so idle scraping can resume where it left off
    last_processed = db.Column(db.Integer, nullable=True)
    status = db.Column(db.String(20), default='pending', nullable=False)  # pending, in_progress, completed, failed, stopped
    created_at = db.Column(db.DateTime, server_default=func.now())
    started_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime, nullable=True)
    error_message = db.Column(db.Text, nullable=True)
    
    @classmethod
    def get_next_pending_task(cls):
        """Get the next pending idle task"""
        return cls.query.filter_by(status='pending').order_by(cls.created_at).first()

    @classmethod
    def claim_next_pending_task(cls):
        """Atomically reserve an idle task across queue-manager processes."""
        task = (
            cls.query.filter_by(status='pending')
            .order_by(cls.created_at)
            .with_for_update(skip_locked=True)
            .first()
        )
        if task is not None:
            task.status = 'in_progress'
            task.started_at = func.now()
            db.session.commit()
        else:
            db.session.rollback()
        return task
    
    @classmethod
    def get_active_task(cls):
        """Get the currently active idle task"""
        return cls.query.filter_by(status='in_progress').first()
    
    def mark_started(self):
        """Mark task as started"""
        self.status = 'in_progress'
        self.started_at = func.now()
        db.session.commit()
    
    def mark_completed(self):
        """Mark task as completed"""
        self.status = 'completed'
        self.completed_at = func.now()
        db.session.commit()
    
    def mark_failed(self, error_message=None):
        """Mark task as failed"""
        self.status = 'failed'
        self.completed_at = func.now()
        if error_message:
            self.error_message = error_message
        db.session.commit()

    def update_progress(self, number):
        """Persist the last processed book number so scraping can resume."""
        try:
            self.last_processed = int(number) if number is not None else None
            db.session.commit()
        except Exception:
            db.session.rollback()
    
    def __repr__(self):
        progress = f"last={self.last_processed}" if self.last_processed is not None else ""
        return f'<IdleTask {self.id} | {self.kod_wydzialu}/{self.start_from}-{self.end_at} {progress} | Status: {self.status}>'


# Helper functions
def get_user_by_login(login):
    """Get user by login/username"""
    return User.query.filter_by(login=login).first()

def get_user(user_id):
    """Get user by ID"""
    return User.query.get(user_id)
