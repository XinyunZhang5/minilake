package io.minilake.actions;

/** One line of a commit file. Each subtype serializes as {"<key>": {...}}. */
public sealed interface Action
    permits Protocol, Metadata, AddFile, RemoveFile, SetTransaction, CommitInfo {}
