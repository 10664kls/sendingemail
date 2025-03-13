package sender

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"
	"gopkg.in/mail.v2"
)

type Service struct {
	mu sync.Mutex

	db   *sql.DB
	zlog *zap.Logger
}

func NewService(_ context.Context, db *sql.DB, zlog *zap.Logger) (*Service, error) {

	return &Service{
		db:   db,
		zlog: zlog,
	}, nil
}

func (s *Service) ListMessages(ctx context.Context) ([]*Message, error) {
	zlog := s.zlog.With(
		zap.String("service", "sender"),
		zap.String("method", "ListMessages"),
	)

	zlog.Info("starting to list messages")

	messages, err := listMailMessages(ctx, s.db)
	if err != nil {
		zlog.Error("failed to list mail messages", zap.Error(err))
		return nil, err
	}
	return messages, nil
}

// Send will be collect an unsent email from wise and
// then send all that to registered email address, this method will
// be use by Cronjob.
func (s *Service) Send(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	zlog := s.zlog.With(
		zap.String("service", "sender"),
		zap.String("method", "Send"),
	)

	rawsMessages, err := listMailMessages(ctx, s.db)
	if err != nil {
		zlog.Error("failed to list mail messages", zap.Error(err))
		return err
	}

	if len(rawsMessages) == 0 {
		zlog.Info("no messages to send")
		return nil
	}

	messages := make([]*mail.Message, 0, len(rawsMessages))
	for _, msg := range rawsMessages {

		m := mail.NewMessage()
		m.SetHeader("From", os.Getenv("MAIL_FROM"))
		m.SetHeader("To", msg.ToAddresses...)
		m.SetHeader("CC", msg.BCCAddresses...)
		m.SetHeader("Subject", msg.Subject)
		m.SetBody("text/html", `<html><body style="font-family: Saysettha OT;">`+msg.Content+`</body></html>`)

		messages = append(messages, m)

	}

	dialer := mail.NewDialer(
		os.Getenv("SMTP_HOST"),
		587,
		os.Getenv("SMTP_USERNAME"),
		os.Getenv("SMTP_PASSWORD"),
	)

	if err := dialer.DialAndSend(messages...); err != nil {
		zlog.Error("failed to send emails", zap.Error(err))
		return err
	}

	for _, msg := range rawsMessages {
		_, err := s.db.ExecContext(ctx, "EXEC dbo.pd_updategetemailwisesend @txnno", sql.Named("txnno", msg.TxnNo))
		if err != nil {
			zlog.Error("failed to update get email wise send", zap.Error(err))
			return err
		}
	}

	zlog.Info("mails sent successfully")
	return nil
}

type Message struct {
	ID     int64
	TxnNo  string
	RuleID string

	// Time is the date of the email
	Time    string
	Subject string
	Content string

	// One of "SEND", "ADD"
	Status       string
	Comment      string
	ToAddresses  []string
	BCCAddresses []string
	SentAt       *time.Time
}

func listMailMessages(ctx context.Context, db *sql.DB) ([]*Message, error) {
	_, err := db.ExecContext(ctx, "EXEC dbo.pd_wiseSendEmail")
	if err != nil {
		return nil, fmt.Errorf("failed to execute stored procedure pd_wiseSendEmail: %w", err)
	}

	q, args := sq.Select(
		"TOP 100 TWID",
		"Txnno",
		"Ruleid",
		"txtdate",
		"toaddress",
		"bccaddress",
		"subjects",
		"contents",
		"rectype",
		"senddatetime",
		"comments",
	).
		From("dbo.tb_getEmailWiseSend").
		PlaceholderFormat(sq.AtP).
		Where(
			sq.Eq{
				"rectype": "ADD",
				"txtdate": time.Now().Format("2006-01-02"),
			},
			sq.NotEq{
				"toaddress": nil,
			}).
		OrderBy("TWID ASC").
		MustSql()

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query tb_getEmailWiseSend: %w", err)
	}
	defer rows.Close()

	ms := make([]*Message, 0)
	for rows.Next() {
		var m Message
		var rawToAddress, rowBccAddress sql.NullString
		if err := rows.Scan(
			&m.ID,
			&m.TxnNo,
			&m.RuleID,
			&m.Time,
			&rawToAddress,
			&rowBccAddress,
			&m.Subject,
			&m.Content,
			&m.Status,
			&m.SentAt,
			&m.Comment,
		); err != nil {
			return nil, fmt.Errorf("failed to scan tb_getEmailWiseSend: %w", err)
		}

		if rawToAddress.Valid {
			toAddresses := strings.FieldsFunc(rawToAddress.String, func(r rune) bool {
				return r == ';'
			})
			m.ToAddresses = toAddresses
		}

		if rowBccAddress.Valid {
			bccAddresses := strings.FieldsFunc(rowBccAddress.String, func(r rune) bool {
				return r == ';'
			})
			m.BCCAddresses = bccAddresses

		}

		ms = append(ms, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate tb_getEmailWiseSend: %w", err)
	}

	return ms, nil
}
