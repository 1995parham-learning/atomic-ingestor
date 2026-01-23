package fileops

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CalculateSHA256 calculates the SHA256 hash of a file
func CalculateSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("read file for hash: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherwise, attempt to create a hard link
// between the two files. If that fails, copy the file contents from src to dst.
func CopyFile(src, dst string) error {
	// Clean paths
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	sfi, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat source: %w", err)
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("non-regular source file %s (%q)", filepath.Base(src), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat destination: %w", err)
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("non-regular destination file %s (%q)", filepath.Base(dst), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return nil
		}
	}
	// Try hard link first (efficient for same filesystem)
	if err = os.Link(src, dst); err == nil {
		return nil
	}
	// Fall back to content copy
	if err := copyFileContents(src, dst); err != nil {
		return fmt.Errorf("copy file contents: %w", err)
	}
	return nil
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all its contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() {
		_ = in.Close()
	}()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	defer func() {
		if cerr := out.Close(); err == nil {
			err = cerr
		}
	}()

	if _, err = io.Copy(out, in); err != nil {
		return fmt.Errorf("copy contents: %w", err)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("sync destination: %w", err)
	}
	return nil
}

// MoveFile moves a file from src to dst atomically when possible.
// It first attempts os.Rename for atomic moves on the same filesystem.
// If that fails (cross-filesystem), it falls back to copy+sync+remove.
func MoveFile(src, dst string) error {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	// Validate source file
	sfi, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat source: %w", err)
	}
	if !sfi.Mode().IsRegular() {
		return fmt.Errorf("MoveFile: non-regular source file %s (%q)", filepath.Base(src), sfi.Mode().String())
	}

	// Try atomic rename first (works on same filesystem)
	if err := os.Rename(src, dst); err == nil {
		return nil
	}

	// Rename failed (likely cross-filesystem), fall back to copy+remove
	// First copy to a temp file, then rename for atomicity
	tmpDst := dst + ".tmp"
	if err := copyFileContents(src, tmpDst); err != nil {
		// Clean up temp file on error (best effort)
		_ = os.Remove(tmpDst)
		return fmt.Errorf("copy file contents: %w", err)
	}

	// Atomic rename of temp file to final destination
	if err := os.Rename(tmpDst, dst); err != nil {
		_ = os.Remove(tmpDst)
		return fmt.Errorf("rename temp to destination: %w", err)
	}

	// Remove source file after successful copy
	if err := os.Remove(src); err != nil {
		// Log but don't fail - the file was successfully copied
		return fmt.Errorf("remove source after copy (destination is safe): %w", err)
	}

	return nil
}
