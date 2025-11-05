package xtdb.segment

import com.carrotsearch.hppc.ByteArrayList
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import xtdb.segment.MergePlannerTest.RenderedMergeTask.Companion.page
import xtdb.segment.MergePlannerTest.RenderedMergeTask.Companion.path
import xtdb.segment.MergePlannerTest.RenderedMergeTask.Companion.render
import xtdb.segment.TestSegment.Companion.seg
import xtdb.segment.TestTrie.Companion.b
import xtdb.segment.TestTrie.Companion.l

class MergePlannerTest {

    data class RenderedMergeTask(val part: ByteArrayList, val pages: List<Page>) {
        data class Page(val segName: String, val pageIdx: Int) {
            override fun toString() = """"$segName" page $pageIdx"""
        }

        fun pages(vararg pages: Page) = copy(pages = pages.toList())

        companion object {
            fun path(vararg path: Byte) = RenderedMergeTask(ByteArrayList.from(*path), emptyList())
            infix fun String.page(pageIdx: Int) = Page(this, pageIdx)

            fun List<MergeTask>.render(): List<RenderedMergeTask> = map { task ->
                RenderedMergeTask(
                    ByteArrayList.from(*task.path),
                    task.pages.map { page -> (page as TestSegment.Page).let { Page(it.name, it.pageIdx) } }
                )
            }
        }

        override fun toString() = "path(${part.toArray().joinToString()}).pages(${pages.joinToString()})"
    }

    @Test
    fun `test merge plan with null nodes #2700`() = runTest {
        withClue("part 1") {
            val t1 = seg("t1", b(b(null, l(0), null, l(1)), l(2), null, l(3)))
            val log = seg("log", l(0))
            val log2 = seg("log2", b(null, null, l(0), l(1)))

            MergePlanner.plan(listOf(t1, log, log2), null).render()
                .shouldBe(
                    listOf(
                        path(0, 0).pages("log" page 0),
                        path(0, 1).pages("t1" page 0, "log" page 0),
                        path(0, 2).pages("log" page 0),
                        path(0, 3).pages("t1" page 1, "log" page 0),
                        path(1).pages("t1" page 2, "log" page 0),
                        path(2).pages("log" page 0, "log2" page 0),
                        path(3).pages("t1" page 3, "log" page 0, "log2" page 1)
                    )
                )
        }

        withClue("part 2") {
            val t1 = seg("t1", b(b(null, l(0), null, l(1)), l(2), null, l(3)))
            val t2 = seg(
                "t2",
                b(
                    b(l(0), l(1), null, b(null, l(2), null, l(3))),
                    null, null,
                    b(null, l(4), null, l(5))
                )
            )

            MergePlanner.plan(listOf(t1, t2), null).render()
                .shouldBe(
                    listOf(
                        path(0, 0).pages("t2" page 0),
                        path(0, 1).pages("t1" page 0, "t2" page 1),
                        path(0, 3, 0).pages("t1" page 1),
                        path(0, 3, 1).pages("t1" page 1, "t2" page 2),
                        path(0, 3, 2).pages("t1" page 1),
                        path(0, 3, 3).pages("t1" page 1, "t2" page 3),
                        path(1).pages("t1" page 2),
                        path(3, 0).pages("t1" page 3),
                        path(3, 1).pages("t1" page 3, "t2" page 4),
                        path(3, 2).pages("t1" page 3),
                        path(3, 3).pages("t1" page 3, "t2" page 5)
                    )
                )
        }
    }
}