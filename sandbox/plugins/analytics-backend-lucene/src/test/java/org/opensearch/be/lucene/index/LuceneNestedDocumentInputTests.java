/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.document.Document;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.List;

/**
 * Tests that {@link LuceneDocumentInput} does NOT index {@code nested} fields (they are stored only in
 * the parquet primary — see design/nested-field-recovery/). The {@code startNestedChild} /
 * {@code endNestedChild} signals must NOT create child documents, must NOT write a {@code _nested_path}
 * term, and any leaf added while a nested scope is open must be dropped for Lucene. The net effect is
 * that every logical row is written as a single flat root document, so segments never carry a
 * {@code __nested_parent} block-join field and the shard recovers cleanly on store recovery.
 */
public class LuceneNestedDocumentInputTests extends LucenePluginBaseTests {

    /** A flat doc emits no nested children: block == [root], hasNestedChildren == false, leaf on root. */
    public void testFlatDocumentProducesSingleRootBlock() {
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(mockKeywordField("status"), "active");

        assertFalse("a flat doc must not report nested children", input.hasNestedChildren());

        List<Document> block = input.getDocumentBlock();
        assertEquals("flat block is just the root", 1, block.size());
        assertSame("the single block entry is the root document", input.getFinalInput(), block.get(0));
        assertNotNull("root keeps its own leaf field", block.get(0).getField("status"));
        assertNull("root never carries a _nested_path", block.get(0).getField(DocumentInput.NESTED_PATH_FIELD));
    }

    /**
     * A nested object contributes NOTHING to Lucene: no child doc, no block, and its leaf fields are
     * dropped (they live in parquet only). The root keeps only its own out-of-scope leaf.
     */
    public void testNestedLeavesAreNotIndexedInLucene() {
        MappedFieldType rootField = mockKeywordField("title");
        MappedFieldType commentAuthor = mockKeywordField("comments.author");

        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(rootField, "post-title"); // outside any nested scope -> root

        input.startNestedChild("comments");
        input.addField(commentAuthor, "alice"); // inside nested scope -> dropped for Lucene
        input.endNestedChild();

        input.startNestedChild("comments");
        input.addField(commentAuthor, "bob"); // inside nested scope -> dropped for Lucene
        input.endNestedChild();

        assertFalse("nested content must not produce a Lucene block", input.hasNestedChildren());
        List<Document> block = input.getDocumentBlock();
        assertEquals("no children — block is just the root", 1, block.size());

        Document root = block.get(0);
        assertSame(input.getFinalInput(), root);
        assertEquals("root keeps its own leaf", "post-title", root.getField("title").stringValue());
        assertNull("nested leaf must not leak onto the root", root.getField("comments.author"));
        assertNull("no _nested_path is ever written", root.getField(DocumentInput.NESTED_PATH_FIELD));
    }

    /**
     * Multi-level nested scopes (depth 3) still produce no Lucene docs: every leaf at every depth is
     * dropped, the scope depth composes correctly, and the block remains just the root.
     */
    public void testMultiLevelNestedScopeSkipped() {
        LuceneDocumentInput input = new LuceneDocumentInput();

        input.startNestedChild("comments");
        input.addField(mockKeywordField("comments.author"), "alice");
        input.startNestedChild("comments.replies");
        input.addField(mockKeywordField("comments.replies.text"), "r1");
        input.startNestedChild("comments.replies.reactions");
        input.addField(mockKeywordField("comments.replies.reactions.emoji"), "smile");
        input.endNestedChild(); // reactions
        input.endNestedChild(); // replies
        input.endNestedChild(); // comments

        assertFalse(input.hasNestedChildren());
        List<Document> block = input.getDocumentBlock();
        assertEquals("no children at any depth — block is just the root", 1, block.size());
        Document root = block.get(0);
        assertNull(root.getField("comments.author"));
        assertNull(root.getField("comments.replies.text"));
        assertNull(root.getField("comments.replies.reactions.emoji"));
        assertNull(root.getField(DocumentInput.NESTED_PATH_FIELD));
    }

    /**
     * The depth counter must return to zero after a balanced open/close, so a flat field added AFTER a
     * nested scope is still indexed on the root (guards against a leaked/stuck depth).
     */
    public void testFlatFieldAfterBalancedNestedScopeIsIndexed() {
        LuceneDocumentInput input = new LuceneDocumentInput();

        input.startNestedChild("comments");
        input.addField(mockKeywordField("comments.author"), "alice"); // dropped
        input.endNestedChild();

        input.addField(mockKeywordField("status"), "active"); // back at depth 0 -> root

        List<Document> block = input.getDocumentBlock();
        assertEquals(1, block.size());
        Document root = block.get(0);
        assertEquals("active", root.getField("status").stringValue());
        assertNull(root.getField("comments.author"));
    }

    /** An empty nested array emits no start/end signals, so the document stays flat. */
    public void testEmptyNestedArrayProducesNoChildDocs() {
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(mockKeywordField("title"), "only-root");

        assertFalse(input.hasNestedChildren());
        List<Document> block = input.getDocumentBlock();
        assertEquals(1, block.size());
        assertSame(input.getFinalInput(), block.get(0));
    }

    /** Closing a nested scope when none is open is a programming error and must fail fast. */
    public void testEndNestedChildWithoutOpenScopeThrows() {
        LuceneDocumentInput input = new LuceneDocumentInput();
        IllegalStateException e = expectThrows(IllegalStateException.class, input::endNestedChild);
        assertTrue(e.getMessage().contains("no open nested scope"));
    }

    /**
     * More closes than opens must also fail fast (depth cannot go negative), guarding the counter against
     * silently absorbing an unbalanced signal stream.
     */
    public void testUnbalancedNestedScopeThrows() {
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.startNestedChild("comments");
        input.endNestedChild();
        expectThrows(IllegalStateException.class, input::endNestedChild);
    }

    /** The row ID is written on the single (root) document; there are no children to correlate. */
    public void testSetRowIdWritesOnRoot() {
        LuceneDocumentInput input = new LuceneDocumentInput();
        input.startNestedChild("comments");
        input.addField(mockKeywordField("comments.author"), "alice");
        input.endNestedChild();
        input.setRowId(DocumentInput.ROW_ID_FIELD, 7L);

        assertEquals(7L, input.getRowId());
        List<Document> block = input.getDocumentBlock();
        assertEquals(1, block.size());
        assertNotNull("root carries __row_id__", block.get(0).getField(DocumentInput.ROW_ID_FIELD));
    }
}
